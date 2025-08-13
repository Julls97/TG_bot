import asyncio
import logging
import sqlite3
import os
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, date
import re
import gspread
from google.oauth2.service_account import Credentials

from poem import TeamPoemManager, TeamPoemState

logging.basicConfig(level=logging.INFO)

load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 874968987

questions = [
    {
        "text": [
            "В каком году была основана компания?",
            "В каком году компания стала резидентом Сколково?",
            "Назовите три ключевые ценности корпоративной культуры, важные для роста нашей компании",
            "Сформулируйте 2-3 ключевых правила для поведения сотрудников на встречах и совещаниях"
        ],
        "time": None
    },
    {
        "text": [
            "Что ты запомнил из сегодняшнего выступления Григорьева Игоря? Напишите ключевую мысль.",
            "Что ты запомнил из сегодняшнего выступления Андреева Дмитрия? Напишите ключевую мысль.",
            "По твоему мнению, какое самое важное достижение у компании за этот год и почему?"
        ],
        # Для тестирования: через 2 минуты после запуска
        "time": datetime.now() + timedelta(minutes=2)
        # Для продакшена:
        #"time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
    },
    {
        "text": [
            "Сделайте и отправьте креативную фотографию с коллегой с которым чаще всего взаимодействуешь по работе (приветствуется использование ИИ)."
        ],
        # Для тестирования: через 4 минуты после запуска
        "time": datetime.now() + timedelta(minutes=3)
        # Для продакшена:
        # "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=12, minutes=30)
    },
    {
        "text": [
            "Какой продукт нашей компании тебе нравится больше всего и почему?\nОпиши, что именно в этом продукте привлекает тебя — будь то функциональность, дизайн, польза для клиентов или что-то ещё. Постарайся раскрыть свои личные впечатления и причины выбора.",
            "С помощью ИИ сгенерируй и направь сюда ответ с нестандартными способами использования продукта, о котором ты писал(а) выше, выходящими за рамки его традиционного применения."
        ],
        # Для тестирования: через 6 минут после запуска
        "time": datetime.now() + timedelta(minutes=4)
        # Для продакшена:
        # "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=14, minutes=30)
    },
    {
        "text": [
            "Если бы вы могли воплотить принципы Agile в образе живого существа или объекта, что бы это было и почему?",
            "Как бы вы переосмыслили одно из ключевых правил Agile, чтобы оно отражало не только гибкость и скорость, но и вдохновение и творческий подход в работе команды?",
            "Расшифруйте ребус из эмодзи и напишите, какое Agile-понятие или практика здесь изображены\n 🐢📅🛠",
        ],
        # Для тестирования: через 8 минут после запуска
        "time": datetime.now() + timedelta(minutes=5)
        # Для продакшена:
        # "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=15, minutes=00)
    },
    {
        "text": [
            "Задание командной творческой цепочки: ""Стих о нашей компании"".\n"
            "Цель — создать совместное стихотворение, отражающее уникальность компании.\n\n"
            "1. Первому участнику команды приходит задание (первый участник команды - это тот кто первый зарегистрировался в чат боте из команды):\n"
            "— Напишите в стихотворной форме одну строчку, посвящённую нашей компании.\n\n"
            "2. Как только первый участник отправляет свою строчку, задание автоматически переходит к следующему участнику:\n"
            "— Продолжите стихотворение, добавив ещё одну рифмованную строчку.\n\n"
            "3. Задание поочерёдно передаётся всем участникам команды, каждый добавляет свою строчку, развивая общее стихотворение."
        ],
        # Для тестирования: через 10 минут после запуска
        "time": datetime.now() + timedelta(minutes=6)
        # Для продакшена:
        # "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=16, minutes=00)
    },
]

class BotState(StatesGroup):
    waiting_for_fio = State()
    waiting_for_team = State()
    waiting_for_run_quiz = State()
    asking = State()
    waiting = State()

class InteractiveBot:
    def __init__(self, token: str):
        self.bot = Bot(token=token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.router = Router()
        self.dp.include_router(self.router)
        self._init_db()
        self.poem_manager = TeamPoemManager(self.bot, self.conn, questions)

        self.bot_active = True

        self.admin_export = AdminExport(
            bot=self.bot,
            cur=self.cur,
            admin_id=ADMIN_ID,
            creds_json_path="config/service_account.json",
            spreadsheet_id="1MQMhgMeI5B1zjK-UcPhVGVBHFer5HUMdnyvs0A5FayU"
        )

        self.keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Красный", callback_data="team_Красный")],
            [InlineKeyboardButton(text="Желтый", callback_data="team_Желтый")],
            [InlineKeyboardButton(text="Зелёный", callback_data="team_Зелёный")],
            [InlineKeyboardButton(text="Синий", callback_data="team_Синий")]
        ])

        self.scheduler = AsyncIOScheduler()
        # Словарь для отслеживания активных блоков пользователей
        self.active_blocks = {}
        self._register_handlers()

    def _init_db(self):
        self.conn = sqlite3.connect('quiz_answers.db')
        self.cur = self.conn.cursor()

        # Проверяем существование таблицы
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='answers'")
        table_exists = self.cur.fetchone()

        if not table_exists:
            # Создаем новую таблицу
            num_questions = sum(len(block["text"]) for block in questions)
            answers_cols = ', '.join([f"answer_{i + 1} TEXT" for i in range(num_questions)])
            self.cur.execute(f"""
                CREATE TABLE answers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    chat_id INTEGER,
                    username TEXT,
                    full_name TEXT,
                    fio TEXT,
                    team TEXT,
                    current_block INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 0,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    {answers_cols}
                )
            """)
        else:
            # Проверяем и добавляем недостающие столбцы
            self.cur.execute("PRAGMA table_info(answers)")
            columns = [column[1] for column in self.cur.fetchall()]

            if 'is_active' not in columns:
                self.cur.execute("ALTER TABLE answers ADD COLUMN is_active INTEGER DEFAULT 0")
                logging.info("Добавлен столбец is_active")

            if 'last_activity' not in columns:
                self.cur.execute("ALTER TABLE answers ADD COLUMN last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                logging.info("Добавлен столбец last_activity")

            if 'current_block' not in columns:
                self.cur.execute("ALTER TABLE answers ADD COLUMN current_block INTEGER DEFAULT 0")
                logging.info("Добавлен столбец current_block")

        self.conn.commit()

    def _register_handlers(self):
        # 1. ОСНОВНЫЕ КОМАНДЫ (самые приоритетные)
        @self.router.message(Command("start"))
        async def cmd_start(message: Message, state: FSMContext):
            await self.name(message)
            await state.set_state(BotState.waiting_for_fio)

        @self.router.message(Command("stop"))
        async def stop_cmd(message: Message, state: FSMContext):
            await state.clear()
            await message.answer("Сессия сброшена.")

        # 2. ВСЕ АДМИНСКИЕ КОМАНДЫ (до обработчиков состояний и универсального обработчика)
        @self.router.message(Command("help_admin"))
        async def admin_help_cmd(message: Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к админ-командам.")
                return
            text = (
                "👩‍💼👨‍💼‍ <b>Доступные команды администратора</b>:\n\n"
                "/results — вывести все результаты пользователей\n"
                "/quiz_list — список блоков вопросов\n"
                "/block [номер_блока] — посмотреть вопросы из выбранного блока\n"
                "/export — выгрузка в таблицу\n"
                "/download_all_photos — выгрузка в таблицу\n"
                "/finish_game — завершить игру досрочно\n"
                "/help_admin — список админ-команд\n"
            )
            await message.answer(text, parse_mode="HTML")

        @self.router.message(Command("results"))
        async def view_results(message: types.Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к просмотру результатов.")
                return
            all_results = self.get_all_answers()
            if not all_results:
                await message.answer("Ответов пока нет.")
                return
            text = ""
            for idx, row in enumerate(all_results, 1):
                user_info = f"{row[3]} (@{row[2]})"
                num_questions = sum(len(block["text"]) for block in questions)
                # Формируем список ответов (начинаем с индекса 4, так как первые 4 колонки - это метаданные)
                answers = []
                for i in range(num_questions):
                    answer = row[10 + i] if (10 + i) < len(row) and row[10 + i] is not None else "Нет ответа"
                    answers.append(f"{i + 1}: {answer}")

                text += f"{idx}. {user_info}\n" + "\n".join(answers) + "\n\n"
            for chunk in [text[i:i + 4000] for i in range(0, len(text), 4000)]:
                await message.answer(chunk)

        @self.router.message(Command("quiz_list"))
        async def list_blocks_cmd(message: Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к просмотру вопросов.")
                return
            text = "Список блоков:\n"
            for idx, block in enumerate(questions):
                block_title = block.get("title", f"Блок №{idx + 1}")
                text += f"{block_title} — {len(block['text'])}\n"
            await message.answer(text)

        @self.router.message(Command("block"))
        async def show_block_idx(message: Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к просмотру блоков заданий.")
                return
            parts = message.text.split()
            if len(parts) != 2 or not parts[1].isdigit():
                await message.answer("Используй: /block <номер_блока>")
                return
            idx = int(parts[1])
            if 0 <= idx < len(questions):
                block = questions[idx]["text"]
                result = "\n".join([f"{i + 1}. {q}" for i, q in enumerate(block)])
                await message.answer(f"Вопросы к блоку #{idx}\n{result}")
            else:
                await message.answer("Нет такого блока.")

        @self.router.message(Command("finish_game"))
        async def finish_game_cmd(message: Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к этой команде.")
                return
            await self.finish_bot_work(message)

        @self.router.message(Command("export"))
        async def export_data(message: Message, state: FSMContext):
            await self.admin_export.export_to_sheet(message)

        @self.router.message(Command("download_all_photos"))
        async def download_all_photos_cmd(message: types.Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к этой команде.")
                return
            self.cur.execute("SELECT * FROM answers")
            rows = self.cur.fetchall()
            columns = [desc[0] for desc in self.cur.description]
            photo_file_ids = []
            for row in rows:
                username = row[3] or "unknown"
                for i, col in enumerate(columns):
                    if col.startswith("answer_") and row[i]:
                        if str(row[i]).startswith("photo_file_id:"):
                            photo_file_id = str(row[i]).split(":", 1)[1]
                            photo_file_ids.append((photo_file_id, username))
            saved = 0
            for file_id, username in photo_file_ids:
                try:
                    await self.download_photo_by_file_id(file_id, username)
                    saved += 1
                except Exception as e:
                    await message.answer(f"Ошибка скачивания: {file_id} — {e}")
            await message.answer(f"Готово! Скачано {saved} изображений.")

        # 3. ОБРАБОТЧИКИ СОСТОЯНИЙ (callback_query должны быть перед message для того же состояния)
        @self.router.callback_query(BotState.waiting_for_team)
        async def setup_team(callback: types.CallbackQuery, state: FSMContext):
            choice = callback.data.split("_")[1]
            data = await state.get_data()
            chat_id, user_id = data["chat_id"], data["user_id"]
            self.cur.execute("UPDATE answers SET team=? WHERE chat_id=? AND user_id=?", (choice, chat_id, user_id))
            self.conn.commit()
            await state.update_data(team=choice)
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(f"Вы выбрали вариант: {choice}")
            await self.run_quiz(callback.message, state)

        @self.router.message(BotState.waiting_for_fio)
        async def registration(message: types.Message, state: FSMContext):
            fio = message.text
            user = message.from_user
            chat_id = message.chat.id

            self.cur.execute("SELECT id FROM answers WHERE user_id=? AND chat_id=?", (user.id, chat_id))
            existing = self.cur.fetchone()

            if existing:
                self.cur.execute(
                    "UPDATE answers SET username=?, full_name=?, fio=?, is_active=0, last_activity=CURRENT_TIMESTAMP WHERE user_id=? AND chat_id=?",
                    (user.username or "", user.full_name or "", fio, user.id, chat_id)
                )
            else:
                self.cur.execute(
                    "INSERT INTO answers (user_id, chat_id, username, full_name, fio, team, current_block, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (user.id, chat_id, user.username or "", user.full_name or "", fio, "", 0, 0)
                )
            self.conn.commit()

            await state.update_data(fio=fio, chat_id=chat_id, user_id=user.id)
            await message.answer(f"Отлично, {fio}")
            await self.team(message, state)

        @self.router.message(BotState.waiting_for_team)
        async def error_on_team(message: Message):
            await message.answer("Пожалуйста, выберите вариант с помощью кнопки!")

        @self.router.callback_query(BotState.waiting_for_run_quiz)
        async def registration_complete(callback: CallbackQuery, state: FSMContext):
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer("Погнали! 🚀")
            await self.start_quiz(callback.message, state)
            if not self.scheduler.running:
                self.schedule_all_blocks()

        @self.router.message(BotState.asking)
        async def next_question(message: types.Message, state: FSMContext):
            await self.process_answer(message, state)

        @self.router.message(TeamPoemState.waiting_for_poem_line)
        async def handle_poem_line(message: types.Message, state: FSMContext):
            success = await self.poem_manager.process_poem_line(message, state)
            if success:
                # Очищаем состояние после успешной обработки
                await state.clear()

        # 4. УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК - ОБЯЗАТЕЛЬНО ПОСЛЕДНИЙ!
        @self.router.message()
        async def handle_message_without_state(message: types.Message, state: FSMContext):
            current_state = await state.get_state()

            if current_state == BotState.asking.state:
                await self.process_answer(message, state)
                return

            self.cur.execute("SELECT is_active, current_block FROM answers WHERE chat_id=? AND user_id=?",
                             (message.chat.id, message.from_user.id))
            result = self.cur.fetchone()

            if result and result[0] == 1:
                user_key = f"{message.chat.id}_{message.from_user.id}"

                if user_key in self.active_blocks:
                    active_block_index = self.active_blocks[user_key]
                    data = await state.get_data()

                    if not data.get("block_questions") or data.get("quiz_index") != active_block_index:
                        questions_block = questions[active_block_index]["text"]
                        await state.set_data({
                            "chat_id": message.chat.id,
                            "user_id": message.from_user.id,
                            "block_questions": questions_block,
                            "block_step": 0,
                            "answers": [],
                            "quiz_index": active_block_index,
                        })
                        logging.info(
                            f"Восстановлено состояние для пользователя {message.from_user.id}, блок {active_block_index}")

                    await state.set_state(BotState.asking)
                    await self.process_answer(message, state)
                else:
                    logging.warning(f"Пользователь {message.from_user.id} активен в БД, но нет активного блока")

    async def download_photo_by_file_id(self, photo_file_id, username):
        file = await self.bot.get_file(photo_file_id)
        file_path = file.file_path
        # Очищаем username от недопустимых для файлов символов
        safe_username = re.sub(r'[^\w.-]', '_', str(username))
        destination = f"downloaded_images/{safe_username}.jpg"
        os.makedirs("downloaded_images", exist_ok=True)
        with open(destination, "wb") as f:
            file_bytes = await self.bot.download_file(file_path)
            f.write(file_bytes.getvalue())
        return destination

# -----------------------------------------------------------------------------------------------------------------
    async def name(self, message: Message):
        await message.answer("Дорогой коллега, приветствую тебя в корпоративной игре, которая проводится в рамках мероприятия «Традиции и трансформация». 🎉")
        await message.answer("Пожалуйста, введите своё ФИ для регистрации участия:")

    async def team(self, message: Message, state: FSMContext):
        await message.answer(f"Теперь выберите цвет своего браслета, так мы сможем закрепить тебя в качестве участника за одной из команд:", reply_markup=self.keyboard)
        await state.set_state(BotState.waiting_for_team)

    async def run_quiz(self, message: types.Message, state: FSMContext):
        keyboard_yes = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="ДА", callback_data="button_pressed")]
        ])
        await message.answer(f"Спасибо за регистрацию! 📋")
        await message.answer(f"Вот основные правила нашего корпоратива:\n\n"
                             f"- внимательно слушай спикеров, если есть вопросы  - поднимай руку\n"
                             f"- отвечайте на вопросы, выполняйте задания\n"
                             f"- за каждый правильный ответ команда получает баллы\n"
                             f"- в конце мероприятия будут призы для команд с наибольшим количеством очков, а также индивидуальные подарки\n\n"
                             f"Если готов(а) жми ДА", reply_markup=keyboard_yes)
        await state.set_state(BotState.waiting_for_run_quiz)

    async def start_quiz(self, message: types.Message, state: FSMContext):
        index = 0
        block = questions[index]["text"]

        user_key = f"{message.chat.id}_{message.from_user.id}"
        self.active_blocks[user_key] = index

        # Обновляем статус в базе данных
        self.cur.execute(
            "UPDATE answers SET current_block=?, is_active=1, last_activity=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
            (index, message.chat.id, message.from_user.id)
        )
        self.conn.commit()

        await state.update_data(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            block_questions=block,
            block_step=0,
            answers=[],
            quiz_index=index
        )
        await message.answer(block[0])
        await state.set_state(BotState.asking)

    async def save_answers(self, message: types.Message, answers, state: FSMContext):
        data = await state.get_data()
        chat_id = message.chat.id
        user_id = message.from_user.id
        index = data.get("quiz_index", 0)

        start_answer_index = sum(len(q["text"]) for q in questions[:index])
        questions_count = len(questions[index]["text"])
        answers_padded = answers + [""] * (questions_count - len(answers))

        set_clause_parts = []
        params = []
        for i in range(questions_count):
            col_num = start_answer_index + i + 1
            set_clause_parts.append(f"answer_{col_num}=?")
            params.append(answers_padded[i])

        set_clause = ', '.join(set_clause_parts)
        params.extend([index + 1, chat_id, user_id])

        self.cur.execute(
            f"UPDATE answers SET {set_clause}, current_block=?, last_activity=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
            params
        )
        self.conn.commit()

    async def finish_bot_work(self, message: Message = None):
        """Завершает работу бота и отправляет финальные сообщения всем участникам"""
        try:
            self.bot_active = False

            # Получаем всех зарегистрированных участников
            self.cur.execute("SELECT DISTINCT chat_id, user_id, fio FROM answers WHERE chat_id IS NOT NULL")
            users = self.cur.fetchall()

            final_message = (
                "Дорогой коллега, благодарим тебя за активное участие в нашей корпоративной игре! 🎊 🎉\n\n"
                "На этом все вопросы и задания завершаются.🚀\n\n"
                "Ожидай результаты и награждение 🏆 — они будут объявлены совсем скоро."
            )

            # Отправляем финальное сообщение всем участникам
            sent_count = 0
            for chat_id, user_id, fio in users:
                try:
                    await self.bot.send_message(chat_id, final_message)
                    sent_count += 1
                except Exception as e:
                    logging.error(f"Ошибка отправки финального сообщения пользователю {fio} ({user_id}): {e}")

            # Останавливаем планировщик
            if self.scheduler.running:
                self.scheduler.shutdown()
                logging.info("Планировщик остановлен")

            # Очищаем активные блоки
            self.active_blocks.clear()

            # Обновляем статус всех пользователей в БД
            self.cur.execute("UPDATE answers SET is_active=0 WHERE is_active=1")
            self.conn.commit()

            if message:
                await message.answer(f"✅ Игра завершена!")

            logging.info(f"Игра завершена. Финальное сообщение отправлено {sent_count} участникам.")

        except Exception as e:
            logging.error(f"Ошибка при завершении игры: {e}")
            if message:
                await message.answer("❌ Произошла ошибка при завершении игры.")

    def schedule_all_blocks(self):
        # ИСПРАВЛЕНИЕ: Используем более частый интервал для проверки (каждые 30 секунд)
        # и запускаем планировщик только если он еще не запущен
        if not self.scheduler.running:
            self.scheduler.start()
            logging.info("Планировщик запущен")

        # Добавляем задачу проверки каждые 30 секунд
        self.job = self.scheduler.add_job(
            self.timer_block_run,
            "interval",
            seconds=30,  # Проверяем каждые 30 секунд
            id="timer_job",  # Добавляем ID для предотвращения дубликатов
            replace_existing=True  # Заменяем существующую задачу если есть
        )
        logging.info("Задача планировщика добавлена")

        # Добавляем задачу для автоматического завершения в 16:30
        finish_time = datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=16, minutes=30)

        self.scheduler.add_job(
            self.auto_finish_game,
            "date",
            run_date=finish_time,
            id="auto_finish_job",
            replace_existing=True
        )
        logging.info(f"Запланировано автоматическое завершение игры на {finish_time.strftime('%H:%M')}")

    async def auto_finish_game(self):
        """Автоматическое завершение игры в запланированное время"""
        logging.info("Автоматическое завершение игры запущено")
        await self.finish_bot_work()

    async def timer_block_run(self):
        """Проверяет и запускает блоки по расписанию"""
        try:
            now = datetime.now()
            logging.info(f"Планировщик проверяет блоки в {now.strftime('%H:%M:%S')}")

            # Получаем всех пользователей
            self.cur.execute(
                "SELECT chat_id, user_id, current_block, is_active FROM answers WHERE current_block IS NOT NULL"
            )
            users_data = self.cur.fetchall()

            if not users_data:
                logging.info("Нет зарегистрированных пользователей")
                return

            # Проверяем каждого пользователя
            for chat_id, user_id, current_block, is_active in users_data:
                # Пропускаем активных пользователей
                if is_active == 1:
                    logging.info(f"Пользователь {user_id} активен, пропускаем")
                    continue

                # Пропускаем пользователей, завершивших все блоки
                if current_block >= len(questions):
                    continue

                # Проверяем, есть ли доступный следующий блок
                next_block_index = current_block
                if next_block_index < len(questions):
                    block = questions[next_block_index]
                    block_time = block.get("time")

                    # Пропускаем блоки без времени (первый блок)
                    if block_time is None:
                        continue

                    # Проверяем, пришло ли время для блока
                    if block_time <= now:
                        logging.info(f"Время для блока {next_block_index} пришло для пользователя {user_id}")
                        await self.send_next_block(chat_id, user_id, next_block_index)
                    else:
                        time_diff = (block_time - now).total_seconds() / 60
                        logging.info(
                            f"До блока {next_block_index} для пользователя {user_id} осталось {time_diff:.1f} минут")

        except Exception as e:
            logging.error(f"Ошибка в timer_block_run: {e}", exc_info=True)

    async def send_next_block(self, chat_id, user_id, block_index):
        """Отправляет следующий блок вопросов пользователю"""
        try:
            user_key = f"{chat_id}_{user_id}"

            # Проверяем, не активен ли уже пользователь
            if user_key in self.active_blocks:
                logging.info(f"Пользователь {user_id} уже активен в блоке {self.active_blocks[user_key]}")
                return

            block = questions[block_index]
            questions_block = block["text"]

            # Помечаем пользователя как активного с правильным индексом блока
            self.active_blocks[user_key] = block_index

            # Создаем новое состояние FSM для пользователя
            state = FSMContext(self.dp.storage, key=("bot", str(chat_id), str(user_id)))

            # Очищаем старое состояние
            await state.clear()

            # Устанавливаем новые данные состояния
            await state.set_data({
                "chat_id": chat_id,
                "user_id": user_id,
                "block_questions": questions_block,
                "block_step": 0,
                "answers": [],
                "quiz_index": block_index,
            })
            await state.set_state(BotState.asking)

            # Обновляем базу данных
            self.cur.execute(
                "UPDATE answers SET is_active=1, last_activity=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
                (chat_id, user_id)
            )
            self.conn.commit()

            # Отправляем сообщения пользователю
            await self.bot.send_message(chat_id, "🔔 Ура! Новый блок вопросов доступен!")
            await self.bot.send_message(chat_id, questions_block[0])

            logging.info(
                f"Блок {block_index} успешно отправлен пользователю {user_id}, вопросов в блоке: {len(questions_block)}")

        except Exception as e:
            logging.error(f"Ошибка при отправке блока {block_index} пользователю {user_id}: {e}", exc_info=True)
            # Убираем пользователя из активных в случае ошибки
            user_key = f"{chat_id}_{user_id}"
            if user_key in self.active_blocks:
                del self.active_blocks[user_key]

    async def try_start_immediate_next_block(self, message: types.Message, state: FSMContext, current_quiz_index: int):
        """
        Пытается немедленно запустить следующий блок, если он доступен по времени.
        Возвращает True, если следующий блок был запущен, False - если нет.
        """
        try:
            now = datetime.now()
            chat_id = message.chat.id
            user_id = message.from_user.id

            # Ищем следующий доступный блок
            for next_index in range(current_quiz_index + 1, len(questions)):
                if not self.bot_active:
                    await message.answer("Бот завершил свою работу.")
                    break

                next_block = questions[next_index]
                block_time = next_block.get("time")

                # Пропускаем блоки без времени
                if block_time is None:
                    continue

                if block_time <= now:
                    # Следующий блок доступен, запускаем его немедленно
                    questions_block = next_block["text"]
                    user_key = f"{chat_id}_{user_id}"

                    # Обновляем активные блоки с правильным индексом
                    self.active_blocks[user_key] = next_index

                    # Очищаем старое состояние и устанавливаем новое
                    await state.clear()
                    await state.set_data({
                        "chat_id": chat_id,
                        "user_id": user_id,
                        "block_questions": questions_block,
                        "block_step": 0,
                        "answers": [],
                        "quiz_index": next_index,
                    })
                    await state.set_state(BotState.asking)

                    # Обновляем базу данных
                    self.cur.execute(
                        "UPDATE answers SET current_block=?, is_active=1, last_activity=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
                        (next_index, chat_id, user_id)
                    )
                    self.conn.commit()

                    # Отправляем сообщение о новом блоке и первый вопрос
                    await message.answer("🔔 Следующий блок вопросов уже доступен!")
                    await message.answer(questions_block[0])

                    logging.info(f"Немедленно запущен блок {next_index} для пользователя {user_id}")
                    return True
                else:
                    # Если следующий блок еще недоступен, прекращаем поиск
                    break

            return False

        except Exception as e:
            logging.error(f"Ошибка в try_start_immediate_next_block для пользователя {user_id}: {e}")
            return False

    async def process_answer(self, message: types.Message, state: FSMContext):
        data = await state.get_data()
        questions_block = data.get("block_questions", [])
        step = data.get("block_step", 0)
        answers = data.get("answers", [])
        quiz_index = data.get("quiz_index", 0)

        if not self.bot_active:
            await message.answer("Бот завершил свою работу.")
            return

        # Проверяем, что у нас есть вопросы и корректный шаг
        if not questions_block or step >= len(questions_block):
            logging.error(f"Ошибка: questions_block пустой или step вне границ. "
                          f"questions_block: {questions_block}, step: {step}")
            await message.answer("Произошла ошибка. Пожалуйста, попробуйте еще раз или обратитесь к администратору.")
            return

        current_question = questions_block[step]

        photo_question_text = (
            "Сделайте и отправьте креативную фотографию с коллегой с которым чаще всего взаимодействуешь по работе (приветствуется использование ИИ)."
        )
        if current_question.strip() == photo_question_text.strip():
            if not message.photo:
                await message.answer("Пожалуйста, отправьте фото 📷")
                return
            answers.append(f"photo_file_id:{message.photo[-1].file_id}")
        else:
            answers.append(message.text)

        step += 1
        if step < len(questions_block):
            await state.update_data(block_step=step, answers=answers)
            await message.answer(questions_block[step])
        else:
            await state.update_data(answers=answers)
            await self.save_answers(message, answers, state)

            # Проверяем, есть ли следующий доступный блок
            next_block_started = await self.try_start_immediate_next_block(message, state, quiz_index)

            # Если это последний блок (стихотворение), запускаем особую логику
            if quiz_index == 4:  # Индекс последнего блока с стихотворением
                # Проверяем, не нужно ли запустить командное стихотворение
                poem_started = await self.poem_manager.check_and_start_poem_for_user(
                    message.from_user.id,
                    message.chat.id
                )

                if poem_started:
                    # Устанавливаем специальное состояние для ожидания строки
                    await state.set_state(TeamPoemState.waiting_for_poem_line)
                    return

            if not next_block_started:
                # Проверяем, все ли блоки завершены
                if quiz_index + 1 >= len(questions):
                    # Все блоки завершены - показываем благодарность
                    await message.answer("🎉 Поздравляем! Вы успешно прошли все блоки корпоративной игры!\n\n"
                                         "Спасибо за активное участие в мероприятии «Традиции и трансформация». "
                                         "Ваши ответы записаны и будут учтены при подведении итогов.\n\n"
                                         "Ожидайте объявления результатов и награждения! 🏆")
                else:
                    # Если следующий блок недоступен, показываем сообщение ожидания
                    next_time = questions[quiz_index + 1]["time"]
                    time_str = next_time.strftime("%H:%M") if next_time else "неизвестное время"
                    await message.answer(f"Спасибо за ваши ответы! Они записаны.\n"
                                         f"Следующий блок вопросов будет доступен в {time_str}. "
                                         f"Я отправлю вам уведомление! ⏰")

                # Помечаем пользователя как неактивного после завершения блока
                user_key = f"{message.chat.id}_{message.from_user.id}"
                if user_key in self.active_blocks:
                    del self.active_blocks[user_key]

                # Обновляем статус в базе данных
                self.cur.execute(
                    "UPDATE answers SET is_active=0, last_activity=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
                    (message.chat.id, message.from_user.id)
                )
                self.conn.commit()

                await state.clear()

    def get_all_answers(self):
        self.cur.execute("SELECT * FROM answers")
        return self.cur.fetchall()

    async def main(self):
        try:
            logging.info("Бот запускается...")
            await self.dp.start_polling(self.bot)
        finally:
            self.conn.close()
            if self.scheduler.running:
                self.scheduler.shutdown()
            logging.info("Бот остановлен")

class AdminExport:
    def __init__(self, bot: Bot, cur: sqlite3.Cursor, admin_id: int,
                 creds_json_path: str, spreadsheet_id: str):
        self.bot = bot
        self.cur = cur
        self.admin_id = admin_id
        self.spreadsheet_id = spreadsheet_id
        self.creds_json_path = creds_json_path

        # Авторизация в Google Sheets
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(self.creds_json_path, scopes=scopes)
        self.gc = gspread.authorize(creds)

    def _get_all_answers_data(self, table_name: str):
        if not table_name.isidentifier():
            raise ValueError("Некорректное имя таблицы!")
        # Берем все строки из таблицы answers
        query = f"SELECT * FROM {table_name}"
        self.cur.execute(query)

        columns = [desc[0] for desc in self.cur.description]
        rows = self.cur.fetchall()
        # Формируем список списков, первая строка - заголовки
        data = [columns]
        for row in rows:
            # Конвертация всех значений в строку (Google Sheets API требует строки/числа)
            data.append([str(cell) if cell is not None else "" for cell in row])
        return data

    async def export_to_sheet(self, message: Message):
        # Проверяем, что пишет админ
        if message.from_user.id != self.admin_id:
            await message.answer("У вас нет прав на выполнение этой команды.")
            return

        try:
            sheet = self.gc.open_by_key(self.spreadsheet_id).sheet1  # Можно выбрать нужный лист
            data = self._get_all_answers_data("answers")
            sheet.clear()  # Чистим лист перед загрузкой
            sheet.update('A1', data)  # Загружаем данные начиная с ячейки A1

            spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            sheet = spreadsheet.get_worksheet(1)
            data = self._get_all_answers_data("poem_contributions")
            sheet.clear()  # Чистим лист перед загрузкой
            sheet.update('A1', data)  # Загружаем данные начиная с ячейки A1

            await message.answer("Данные успешно экспортированы в Google Таблицу.")
        except Exception as e:
            await message.answer(f"Произошла ошибка при экспорте: {e}")
            logging.exception("Ошибка при экспорте в Google Sheets")


if __name__ == "__main__":
    tg_bot = InteractiveBot(API_TOKEN)
    asyncio.run(tg_bot.main())