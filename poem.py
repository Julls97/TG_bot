import asyncio
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum

from aiogram import Bot, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup


# ==================== DATACLASSES И ENUMS ====================

class PoemStatus(Enum):
    """Статусы процесса создания стихотворения"""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class TeamMember:
    """Информация об участнике команды"""
    user_id: int
    chat_id: int
    fio: str
    username: str
    order: int  # Порядок в очереди
    has_contributed: bool = False
    contribution: str = ""
    skipped: bool = False  # Добавлено для отслеживания пропусков


@dataclass
class TeamPoem:
    """Состояние командного стихотворения"""
    team: str
    status: PoemStatus = PoemStatus.NOT_STARTED
    members: List[TeamMember] = field(default_factory=list)
    lines: List[str] = field(default_factory=list)
    current_member_index: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    _ready_for_completion: bool = False

    def get_current_member(self) -> Optional[TeamMember]:
        """Получить текущего участника в очереди"""
        # Проверяем, что индекс находится в допустимых пределах
        if 0 <= self.current_member_index < len(self.members):
            return self.members[self.current_member_index]
        
        # Если индекс вышел за пределы, возвращаем None
        logging.debug(f"current_member_index {self.current_member_index} вышел за пределы [0, {len(self.members)})")
        return None

    def get_poem_text(self) -> str:
        """Получить текущий текст стихотворения"""
        if not self.lines:
            return "Стихотворение еще не начато..."

        poem_text = "📜 **Наше командное стихотворение:**\n\n"
        for i, line in enumerate(self.lines, 1):
            poem_text += f"{i}. {line}\n"
        
        # Добавляем информацию о пропущенных участниках
        skipped_members = [m for m in self.members if m.skipped and not m.has_contributed]
        if skipped_members:
            poem_text += "\n⚠️ **Пропустили свой ход:**\n"
            for member in skipped_members:
                poem_text += f"• {member.fio}\n"
        
        return poem_text

    def add_line(self, line: str, member: TeamMember):
        """Добавить строку в стихотворение"""
        self.lines.append(line)
        member.has_contributed = True
        member.contribution = line
        
        # Если это первая строка, устанавливаем статус IN_PROGRESS
        if len(self.lines) == 1:
            self.status = PoemStatus.IN_PROGRESS
        
        # Проверяем, является ли текущий участник последним по индексу
        is_last_member = (self.current_member_index == len(self.members) - 1)
        
        if not is_last_member:
            # Переходим к следующему участнику
            self.current_member_index += 1
        else:
            # Это был последний участник, стихотворение готово к завершению
            logging.info(f"Последний участник {member.fio} добавил строку, стихотворение готово к завершению")
            # Устанавливаем статус готовности к завершению
            self.status = PoemStatus.IN_PROGRESS  # Остается IN_PROGRESS, но готово к завершению
            # Устанавливаем current_member_index в -1, чтобы _process_next_member завершил стихотворение
            self.current_member_index = -1
            # Помечаем стихотворение как готовое к завершению
            self._ready_for_completion = True

    def skip_member(self, member: TeamMember):
        """Пропустить участника"""
        member.skipped = True
        member.has_contributed = False
        
        # Проверяем, является ли текущий участник последним по индексу
        is_last_member = (self.current_member_index == len(self.members) - 1)
        
        if not is_last_member:
            # Переходим к следующему участнику
            self.current_member_index += 1
        else:
            # Это был последний участник, стихотворение готово к завершению
            logging.info(f"Последний участник {member.fio} пропущен, стихотворение готово к завершению")
            # Устанавливаем current_member_index в -1, чтобы _process_next_member завершил стихотворение
            self.current_member_index = -1
            # Помечаем стихотворение как готовое к завершению
            self._ready_for_completion = True


# ==================== СОСТОЯНИЯ FSM ====================

class TeamPoemState(StatesGroup):
    """Состояния для процесса создания командного стихотворения"""
    waiting_for_poem_line = State()  # Ожидание строки от участника


# ==================== ОСНОВНОЙ КЛАСС ====================

class TeamPoemManager:
    """
    Менеджер для управления процессом создания командного стихотворения.
    Обеспечивает последовательное получение строк от участников команды.
    """

    def __init__(self, bot: Bot, db_connection: sqlite3.Connection, dp=None):
        self.bot = bot
        self.conn = db_connection
        self.cur = db_connection.cursor()
        self.dp = dp  # Сохраняем ссылку на Dispatcher для управления состояниями

        # Хранилище состояний стихотворений по командам
        self.team_poems: Dict[str, TeamPoem] = {}

        # Маппинг user_id -> team для быстрого поиска
        self.user_to_team: Dict[int, str] = {}

        # Инициализация таблицы для хранения стихотворений
        self._init_poem_table()

        # Таймаут для ожидания ответа (в минутах)
        self.response_timeout = 2

        # Активные таймеры для участников
        self.active_timers: Dict[int, asyncio.Task] = {}

        logging.info("TeamPoemManager инициализирован")

    def _init_poem_table(self):
        """Создание таблицы для хранения командных стихотворений"""
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS team_poems (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team TEXT NOT NULL,
                status TEXT NOT NULL,
                poem_data TEXT NOT NULL, -- JSON с полными данными
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица для индивидуальных вкладов
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS poem_contributions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                fio TEXT,
                line_number INTEGER,
                contribution TEXT,
                contributed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()
        logging.info("Таблицы для стихотворений созданы")

    async def check_team_readiness_and_start(self, team: str) -> bool:
        """
        Проверяет готовность всех участников команды и запускает стихотворение.

        Args:
            team: Название команды (цвет)

        Returns:
            bool: True если процесс запущен или уже запущен ранее
        """
        try:
            logging.info(f"Проверка готовности команды {team} к стихотворению")
            
            # Проверяем, не запущен ли уже процесс для этой команды
            if team in self.team_poems:
                if self.team_poems[team].status == PoemStatus.IN_PROGRESS:
                    logging.info(f"Процесс стихотворения для команды {team} уже запущен")
                    return True  # Возвращаем True, так как процесс уже идёт
                elif self.team_poems[team].status == PoemStatus.COMPLETED:
                    logging.info(f"Стихотворение команды {team} уже завершено")
                    return True  # Возвращаем True, так как задание выполнено

            # Проверяем, все ли участники команды готовы (достигли блока 5)
            self.cur.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN current_block >= 5 THEN 1 ELSE 0 END) as ready
                FROM answers
                WHERE team = ?
            """, (team,))

            result = self.cur.fetchone()
            if not result:
                logging.warning(f"Нет данных о команде {team} в БД")
                return False

            total, ready = result
            logging.info(f"Команда {team}: {ready}/{total} участников готовы к стихотворению")

            # Запускаем только когда ВСЕ участники готовы
            if ready == total and total > 0:
                logging.info(f"Все участники команды {team} готовы ({ready}/{total}). Запускаем стихотворение.")
                return await self.start_team_poem_block(team)
            else:
                logging.info(f"Команда {team}: только {ready}/{total} участников готовы к стихотворению")
                return False

        except Exception as e:
            logging.error(f"Ошибка при проверке готовности команды {team}: {e}", exc_info=True)
            return False

    async def start_team_poem_block(self, team: str) -> bool:
        """
        Запуск процесса создания стихотворения для команды.

        Args:
            team: Название команды (цвет)

        Returns:
            bool: True если процесс успешно запущен
        """
        try:
            logging.info(f"Запуск процесса стихотворения для команды {team}")
            
            # Проверяем, не запущен ли уже процесс для этой команды
            if team in self.team_poems and self.team_poems[team].status == PoemStatus.IN_PROGRESS:
                logging.warning(f"Процесс стихотворения для команды {team} уже запущен")
                return False

            # Получаем всех участников команды в порядке регистрации
            members = self._get_team_members(team)
            logging.info(f"Получено {len(members)} участников для команды {team}")

            if not members:
                logging.warning(f"Нет участников в команде {team}")
                return False

            # Создаем объект стихотворения
            poem = TeamPoem(
                team=team,
                status=PoemStatus.IN_PROGRESS,
                members=members,
                started_at=datetime.now()
            )

            self.team_poems[team] = poem

            # Обновляем маппинг пользователей
            for member in members:
                self.user_to_team[member.user_id] = team
                logging.info(f"Добавлен участник {member.fio} (user_id: {member.user_id}) в команду {team}")

            # Отправляем инструкцию всем участникам команды
            await self._send_instructions_to_team(poem)

            # Запускаем процесс с первым участником
            await self._request_line_from_member(poem.members[0], poem)

            # Сохраняем в БД
            self._save_poem_state(poem)

            logging.info(f"Процесс стихотворения успешно запущен для команды {team} с {len(members)} участниками")
            return True

        except Exception as e:
            logging.error(f"Ошибка при запуске стихотворения для команды {team}: {e}", exc_info=True)
            return False

    def _get_team_members(self, team: str) -> List[TeamMember]:
        """Получить список участников команды из БД"""
        logging.info(f"🎭 [POEM] Поиск участников команды {team} с current_block >= 5")
        
        self.cur.execute("""
            SELECT user_id, chat_id, fio, username, current_block
            FROM answers
            WHERE team = ? --AND current_block >= 5
            ORDER BY id -- Порядок регистрации
        """, (team,))

        rows = self.cur.fetchall()
        logging.info(f"🎭 [POEM] Найдено {len(rows)} участников команды {team} с current_block >= 5")
        
        # Логируем детали для отладки
        for row in rows:
            logging.info(f"🎭 [POEM] Участник: user_id={row[0]}, chat_id={row[1]}, fio={row[2]}, username={row[3]}, current_block={row[4]}")

        members = []
        for i, row in enumerate(rows):
            members.append(TeamMember(
                user_id=row[0],
                chat_id=row[1],
                fio=row[2] or "Участник",
                username=row[3] or "",
                order=i
            ))

        logging.info(f"🎭 [POEM] Создано {len(members)} объектов TeamMember для команды {team}")
        return members

    async def _send_instructions_to_team(self, poem: TeamPoem):
        """Отправить инструкции всем участникам команды"""
        instruction_text = (
            "🎭 **КОМАНДНОЕ ЗАДАНИЕ: СТИХОТВОРЕНИЕ О КОМПАНИИ**\n\n"
            f"Команда: {poem.team}\n"
            f"Участников: {len(poem.members)}\n\n"
            "📋 **Правила:**\n"
            "1️⃣ Каждый участник пишет ОДНУ строку стихотворения\n"
            "2️⃣ Строки добавляются по очереди\n"
            "3️⃣ Вы увидите все предыдущие строки перед написанием своей\n"
            "4️⃣ Постарайтесь сохранить рифму и ритм\n"
            f"5️⃣ У вас есть {self.response_timeout} минуты на ответ\n\n"
            "🔄 **Порядок участников:**\n"
        )

        # Добавляем список участников
        for i, member in enumerate(poem.members, 1):
            status = "✅" if member.has_contributed else "⏳"
            instruction_text += f"{i}. {member.fio} {status}\n"

        instruction_text += "\n💫 Удачи в творчестве!"

        # Отправляем всем участникам
        for member in poem.members:
            try:
                await self.bot.send_message(
                    member.chat_id,
                    instruction_text,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logging.error(f"Не удалось отправить инструкцию участнику {member.user_id}: {e}")

    async def _request_line_from_member(self, member: TeamMember, poem: TeamPoem):
        """Запросить строку у конкретного участника"""
        try:
            # Устанавливаем состояние FSM для участника
            if self.dp:
                from aiogram.fsm.context import FSMContext
                state = FSMContext(self.dp.storage, key=("bot", str(member.chat_id), str(member.user_id)))
                # Очищаем старое состояние перед установкой нового
                await state.clear()
                await state.set_state(TeamPoemState.waiting_for_poem_line)
                await state.set_data({
                    "team": poem.team,
                    "waiting_for_poem": True,
                    "poem_member_id": member.user_id  # Добавляем идентификатор участника
                })
                logging.info(f"Установлено состояние TeamPoemState.waiting_for_poem_line для user_id={member.user_id}")

            # Формируем сообщение с текущим стихотворением
            message_text = f"🖊 **{member.fio}, ваша очередь!**\n\n"

            if poem.lines:
                message_text += poem.get_poem_text() + "\n"
            else:
                message_text += "Вы начинаете стихотворение о нашей компании!\n\n"

            message_text += (
                "✍️ **Напишите одну строку стихотворения.**\n"
                "Постарайтесь продолжить мысль и сохранить рифму.\n\n"
                f"⏰ У вас есть {self.response_timeout} минуты на ответ."
            )

            await self.bot.send_message(
                member.chat_id,
                message_text,
                parse_mode="Markdown"
            )

            # Обновляем БД - помечаем пользователя как активного
            self.cur.execute(
                "UPDATE answers SET is_active=1 WHERE user_id=? AND chat_id=?",
                (member.user_id, member.chat_id)
            )
            self.conn.commit()

            # Запускаем таймер ожидания
            timer_task = asyncio.create_task(
                self._timeout_handler(member, poem)
            )
            self.active_timers[member.user_id] = timer_task

            logging.info(f"Запрошена строка у участника {member.fio} (user_id: {member.user_id})")

        except Exception as e:
            logging.error(f"Ошибка при запросе строки у участника {member.user_id}: {e}")
            # Пропускаем участника и переходим к следующему
            poem.skip_member(member)
            await self._process_next_member(poem)

    async def _timeout_handler(self, member: TeamMember, poem: TeamPoem):
        """Обработчик таймаута для участника"""
        try:
            await asyncio.sleep(self.response_timeout * 60)

            # Проверяем, не ответил ли участник
            if not member.has_contributed and not member.skipped:
                logging.warning(f"Таймаут для участника {member.fio} (user_id: {member.user_id})")

                # Помечаем участника как пропущенного
                poem.skip_member(member)

                # Добавляем пропуск в стихотворение
                skip_line = f"[Пропущено участником {member.fio}]"
                poem.lines.append(skip_line)

                # Уведомляем участника
                try:
                    await self.bot.send_message(
                        member.chat_id,
                        "⏰ Время истекло! Ваш ход пропущен.\n"
                        "Передаем слово следующему участнику.",
                        parse_mode="Markdown"
                    )
                except:
                    pass

                # Переходим к следующему или завершаем стихотворение
                await self._process_next_member(poem)

        except asyncio.CancelledError:
            logging.info(f"Таймер для участника {member.user_id} отменен")

    async def process_poem_line(self, message: types.Message, state: FSMContext) -> bool:
        """
        Обработка строки стихотворения от участника.

        Args:
            message: Сообщение с текстом строки
            state: Состояние FSM

        Returns:
            bool: True если строка успешно обработана
        """
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id

            # Проверяем состояние FSM
            state_data = await state.get_data()
            logging.info(f"Обработка строки стихотворения от user_id={user_id}, state_data={state_data}")

            # Находим команду участника
            team = self.user_to_team.get(user_id)
            if not team or team not in self.team_poems:
                logging.warning(f"Пользователь {user_id} не найден в команде или команда не активна")
                await message.answer("❌ Вы не участвуете в создании стихотворения.")
                return False

            poem = self.team_poems[team]

            # Проверяем, что это очередь данного участника
            current_member = poem.get_current_member()
            if not current_member or current_member.user_id != user_id:
                logging.warning(f"Пользователь {user_id} пытается ответить не в свою очередь. Текущий: {current_member.user_id if current_member else 'None'}")
                await message.answer(
                    "⏳ Сейчас не ваша очередь. Дождитесь своего хода."
                )
                return False

            # Дополнительная проверка: убеждаемся, что участник действительно должен отвечать
            if current_member.has_contributed or current_member.skipped:
                logging.warning(f"Пользователь {user_id} уже ответил или был пропущен")
                await message.answer("❌ Вы уже ответили на этот вопрос.")
                return False

            # Проверяем, что участник действительно является текущим в очереди
            if poem.current_member_index < 0 or poem.current_member_index >= len(poem.members):
                logging.warning(f"Некорректный current_member_index: {poem.current_member_index} для команды с {len(poem.members)} участниками")
                await message.answer("❌ Произошла ошибка в процессе. Попробуйте еще раз.")
                return False

            # Отменяем таймер
            if user_id in self.active_timers:
                self.active_timers[user_id].cancel()
                del self.active_timers[user_id]

            # Добавляем строку в стихотворение
            line_text = message.text.strip()
            if not line_text:
                await message.answer("❌ Строка не может быть пустой. Попробуйте еще раз.")
                return False

            poem.add_line(line_text, current_member)

            # Сохраняем в БД
            self._save_contribution(poem.team, current_member, line_text, len(poem.lines))

            # Отправляем подтверждение
            await message.answer(
                "✅ Ваша строка добавлена в стихотворение!\n"
                "Спасибо за ваш вклад! 🎭"
            )

            # Уведомляем всю команду о прогрессе
            await self._notify_team_progress(poem, current_member)

            # Переходим к следующему участнику или завершаем
            completed_user_ids = await self._process_next_member(poem)

            logging.info(f"Строка стихотворения от пользователя {user_id} успешно обработана: '{line_text}'")
            
            # Возвращаем информацию о завершивших пользователях
            return completed_user_ids if completed_user_ids else True

        except Exception as e:
            logging.error(f"Ошибка при обработке строки стихотворения от user_id={user_id}: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте еще раз.")
            return False

    async def _process_next_member(self, poem: TeamPoem):
        """Переход к следующему участника или завершение стихотворения"""
        try:
            # Проверяем, есть ли еще участники
            next_member = poem.get_current_member()

            if next_member:
                # Запрашиваем строку у следующего участника
                await self._request_line_from_member(next_member, poem)
            else:
                # Завершаем стихотворение если:
                # 1. Все участники обработаны, или
                # 2. Стихотворение помечено как готовое к завершению, или
                # 3. current_member_index вышел за пределы (установлен в -1)
                all_processed = all(member.has_contributed or member.skipped for member in poem.members)
                index_out_of_bounds = poem.current_member_index < 0 or poem.current_member_index >= len(poem.members)
                
                if all_processed or poem._ready_for_completion or index_out_of_bounds:
                    logging.info(f"Стихотворение команды {poem.team} готово к завершению. "
                               f"Все обработаны: {all_processed}, готово к завершению: {poem._ready_for_completion}, "
                               f"индекс вне границ: {index_out_of_bounds}")
                    completed_user_ids = await self._complete_team_poem(poem)
                    return completed_user_ids
                else:
                    logging.warning(f"Стихотворение команды {poem.team} не может быть завершено - "
                                  f"не все участники обработаны и не готово к завершению")
                    return []

        except Exception as e:
            logging.error(f"Ошибка при переходе к следующему участнику: {e}")
            return []

    async def _complete_team_poem(self, poem: TeamPoem):
        """Завершение создания командного стихотворения"""
        try:
            poem.status = PoemStatus.COMPLETED
            poem.completed_at = datetime.now()

            # Отменяем все активные таймеры для этой команды
            for member in poem.members:
                if member.user_id in self.active_timers:
                    self.active_timers[member.user_id].cancel()
                    del self.active_timers[member.user_id]

            # Формируем финальное сообщение
            completion_text = (
                    "🎉 **СТИХОТВОРЕНИЕ ЗАВЕРШЕНО!**\n\n"
                    f"Команда {poem.team} успешно создала стихотворение!\n\n"
                    + poem.get_poem_text() + "\n\n"
                                             "👏 **Авторы:**\n"
            )

            # Добавляем список авторов (включая тех, кто ответил)
            for member in poem.members:
                if member.has_contributed:
                    completion_text += f"• {member.fio}\n"

            completion_text += "\n🏆 Отличная командная работа!"

            # Отправляем всем участникам команды
            for member in poem.members:
                try:
                    await self.bot.send_message(
                        member.chat_id,
                        completion_text,
                        parse_mode="Markdown"
                    )

                    # Обновляем состояние участника в БД - помечаем как завершившего все задания
                    self.cur.execute(
                        "UPDATE answers SET current_block=6, is_active=0 WHERE user_id=? AND chat_id=?",
                        (member.user_id, member.chat_id)
                    )

                    # Отправляем финальное сообщение о завершении всех блоков
                    await self.bot.send_message(
                        member.chat_id,
                        "🎊 Поздравляем! Вы успешно прошли все блоки корпоративной игры!\n\n"
                        "Спасибо за активное участие в мероприятии «Традиции и трансформация». "
                        "Ваши ответы записаны и будут учтены при подведении итогов.\n\n"
                        "Ожидайте объявления результатов и награждения! 🏆"
                    )

                except Exception as e:
                    logging.error(f"Не удалось отправить финал участнику {member.user_id}: {e}")

            self.conn.commit()

            # Сохраняем финальное состояние в БД
            self._save_poem_state(poem)

            # Очищаем данные
            del self.team_poems[poem.team]
            for member in poem.members:
                self.user_to_team.pop(member.user_id, None)

            # Возвращаем список пользователей для очистки из active_blocks
            return [member.user_id for member in poem.members]

            logging.info(f"Стихотворение команды {poem.team} успешно завершено")

        except Exception as e:
            logging.error(f"Ошибка при завершении стихотворения: {e}")

    async def _notify_team_progress(self, poem: TeamPoem, last_contributor: TeamMember):
        """Уведомить команду о прогрессе создания стихотворения"""
        progress_text = (
            f"📝 **Обновление стихотворения**\n\n"
            f"{last_contributor.fio} добавил(а) строку:\n"
            f"➡️ _{poem.lines[-1]}_\n\n"
            f"Строк написано: {len(poem.lines)}/{len(poem.members)}"
        )

        # Отправляем всем, кроме автора последней строки
        for member in poem.members:
            if member.user_id != last_contributor.user_id:
                try:
                    await self.bot.send_message(
                        member.chat_id,
                        progress_text,
                        parse_mode="Markdown"
                    )
                except:
                    pass

    def _save_poem_state(self, poem: TeamPoem):
        """Сохранить состояние стихотворения в БД"""
        try:
            poem_data = {
                'team': poem.team,
                'status': poem.status.value,
                'lines': poem.lines,
                'members': [
                    {
                        'user_id': m.user_id,
                        'fio': m.fio,
                        'has_contributed': m.has_contributed,
                        'contribution': m.contribution,
                        'skipped': m.skipped
                    }
                    for m in poem.members
                ],
                'current_member_index': poem.current_member_index
            }

            self.cur.execute("""
                INSERT OR REPLACE INTO team_poems 
                (team, status, poem_data, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                poem.team,
                poem.status.value,
                json.dumps(poem_data, ensure_ascii=False),
                poem.started_at,
                poem.completed_at
            ))

            self.conn.commit()

        except Exception as e:
            logging.error(f"Ошибка при сохранении состояния стихотворения: {e}")

    def _save_contribution(self, team: str, member: TeamMember, line: str, line_number: int):
        """Сохранить индивидуальный вклад в БД"""
        try:
            self.cur.execute("""
                INSERT INTO poem_contributions
                (team, user_id, chat_id, fio, line_number, contribution)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (team, member.user_id, member.chat_id, member.fio, line_number, line))

            self.conn.commit()

        except Exception as e:
            logging.error(f"Ошибка при сохранении вклада: {e}")

    async def reset_user_poem_state(self, user_id: int, chat_id: int) -> bool:
        """
        Сбросить состояние пользователя в стихотворении.
        Используется для перезапуска задания.

        Args:
            user_id: ID пользователя
            chat_id: ID чата

        Returns:
            bool: True если состояние успешно сброшено
        """
        try:
            # Находим команду пользователя
            team = self.user_to_team.get(user_id)
            if not team or team not in self.team_poems:
                return False

            poem = self.team_poems[team]

            # Находим участника
            current_member = None
            for member in poem.members:
                if member.user_id == user_id:
                    current_member = member
                    break

            if not current_member:
                return False

            # Отменяем таймер если есть
            if user_id in self.active_timers:
                self.active_timers[user_id].cancel()
                del self.active_timers[user_id]

            # Если это текущий участник, запрашиваем строку заново
            if poem.get_current_member() and poem.get_current_member().user_id == user_id:
                await self._request_line_from_member(current_member, poem)
                return True

            return False

        except Exception as e:
            logging.error(f"Ошибка при сбросе состояния пользователя {user_id}: {e}")
            return False

    def is_user_in_poem_process(self, user_id: int) -> bool:
        """
        Проверить, участвует ли пользователь в процессе создания стихотворения.

        Args:
            user_id: ID пользователя

        Returns:
            bool: True если пользователь участвует в активном процессе
        """
        team = self.user_to_team.get(user_id)
        if not team or team not in self.team_poems:
            logging.info(f"🎭 [POEM] Проверка участия пользователя {user_id} в процессе: False (нет команды или команда не активна)")
            return False

        poem = self.team_poems[team]
        is_in_process = poem.status == PoemStatus.IN_PROGRESS
        
        if is_in_process:
            current_member = poem.get_current_member()
            logging.info(f"🎭 [POEM] Пользователь {user_id} в команде {team}, текущий участник: {current_member.user_id if current_member else 'None'}")
        
        return is_in_process

    def is_team_poem_active(self, team: str) -> bool:
        """
        Проверить, активен ли процесс стихотворения для команды.

        Args:
            team: Название команды

        Returns:
            bool: True если процесс активен или завершен
        """
        if team not in self.team_poems:
            return False
        return self.team_poems[team].status in [PoemStatus.IN_PROGRESS, PoemStatus.COMPLETED]

    def get_team_poem_stats(self, team: str) -> Dict:
        """Получить статистику по стихотворению команды"""
        try:
            self.cur.execute("""
                SELECT status, poem_data, started_at, completed_at
                FROM team_poems
                WHERE team = ?
                ORDER BY created_at DESC LIMIT 1
            """, (team,))

            result = self.cur.fetchone()
            if result:
                poem_data = json.loads(result[1])
                return {
                    'status': result[0],
                    'lines_count': len(poem_data.get('lines', [])),
                    'members_count': len(poem_data.get('members', [])),
                    'started_at': result[2],
                    'completed_at': result[3],
                    'lines': poem_data.get('lines', [])
                }

            return None

        except Exception as e:
            logging.error(f"Ошибка при получении статистики: {e}")
            return None