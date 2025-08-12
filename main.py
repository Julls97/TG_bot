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
        "time": datetime.now()
    },
    {
        "text": [
            "Что ты запомнил из сегодняшнего выступления Григорьева Игоря? Напишите ключевую мысль.",
            "Что ты запомнил из сегодняшнего выступления Андреева Дмитрия? Напишите ключевую мысль.",
            "По твоему мнению, какое самое важное достижение у компании за этот год и почему?"
        ],
        "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
    },
    {
        "text": [
            "Сделайте и отправьте креативную фотографию с коллегой с которым чаще всего взаимодействуешь по работе (приветствуется использование ИИ)."
        ],
        "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
    },
    {
        "text": [
            "Какой продукт нашей компании тебе нравится больше всего и почему?\nОпиши, что именно в этом продукте привлекает тебя — будь то функциональность, дизайн, польза для клиентов или что-то ещё. Постарайся раскрыть свои личные впечатления и причины выбора.",
            "С помощью ИИ сгенерируй и направь сюда ответ с нестандартными способами использования продукта, о котором ты писал(а) выше, выходящими за рамки его традиционного применения."
        ],
        "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
    },
    {
        "text": [
            "Если бы вы могли воплотить принципы Agile в образе живого существа или объекта, что бы это было и почему?",
            "Как бы вы переосмыслили одно из ключевых правил Agile, чтобы оно отражало не только гибкость и скорость, но и вдохновение и творческий подход в работе команды?",
            "Расшифруйте ребус из эмодзи и напишите, какое Agile-понятие или практика здесь изображены\n 🐢📅🛠",
        ],
        "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
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
        "time": datetime.combine(date.today(), datetime.min.time()) + timedelta(hours=9, minutes=55)
    },
]

class State(StatesGroup):
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
        self._register_handlers()

    def _init_db(self):
        self.conn = sqlite3.connect('quiz_answers.db')
        self.cur = self.conn.cursor()
        num_questions = sum(len(block["text"]) for block in questions)
        answers_cols = ', '.join([f"answer_{i+1} TEXT" for i in range(num_questions)])
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                username TEXT,
                full_name TEXT,
                fio TEXT,
                team TEXT,
                current_block INTEGER,
                {answers_cols}
            )
        """)
        self.conn.commit()

    def _register_handlers(self):
        @self.router.message(Command("start"))
        async def cmd_start(message: Message, state: FSMContext):
            await self.name(message)
            await state.set_state(State.waiting_for_fio)

        @self.router.message(Command("stop"))
        async def stop_cmd(message: Message, state: FSMContext):
            await state.clear()
            await message.answer("Сессия сброшена.")

        @self.router.message(State.waiting_for_fio)
        async def registration(message: types.Message, state: FSMContext):
            fio = message.text
            user = message.from_user
            chat_id = message.chat.id
            self.cur.execute(
                "INSERT INTO answers (user_id, chat_id, username, full_name, fio, team) VALUES (?, ?, ?, ?, ?, ?)",
                (user.id, chat_id, user.username or "", user.full_name or "", fio, "")
            )
            self.conn.commit()
            await state.update_data(
                fio=fio, chat_id=chat_id, user_id=user.id
            )
            await message.answer(f"Отлично, {fio}")
            await self.team(message, state)

        @self.router.callback_query(State.waiting_for_team)
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

        @self.router.message(State.waiting_for_team)
        async def error_on_team(message: Message):
            # Если человек отправил текст, а не нажал кнопку
            await message.answer("Пожалуйста, выберите вариант с помощью кнопки!")

        @self.router.callback_query(State.waiting_for_run_quiz)
        async def registration_complete(callback: CallbackQuery, state: FSMContext):
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer("Погнали! 🚀")
            await self.start_quiz(callback.message, state)
            self.schedule_all_blocks()

        @self.router.message(State.asking)
        async def next_question(message: types.Message, state: FSMContext):
            data = await state.get_data()
            questions_block = data.get("block_questions", [])
            step = data.get("block_step", 0)
            answers = data.get("answers", [])
            quiz_index = data.get("quiz_index", 0)

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
                await message.answer("Спасибо за ваши ответы! Они записаны. Ждите следующий блок по расписанию.")

                next_quiz_index = quiz_index + 1
                now = datetime.now()
                if next_quiz_index < len(questions) and questions[next_quiz_index]["time"] <= now:
                    next_block_data = questions[next_quiz_index]["text"]
                    await state.clear()
                    await state.update_data(
                        chat_id=message.chat.id,
                        user_id=message.from_user.id,
                        block_questions=next_block_data,
                        block_step=0,
                        answers=[],
                        quiz_index=next_quiz_index
                    )
                    await state.set_state(State.asking)
                    await message.answer(next_block_data[0])
                else:
                    await state.clear()

# -----------------------------------------------------------------------------------------------------------------
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
                "/run_block — запустить блок опроса вручную\n"
                "/remind — тестовое напоминание по времени\n"
                "/export — выгрузка в таблицу\n"
                "/download_all_photos — выгрузка в таблицу\n"
                # Добавьте свои команды ниже по мере необходимости:
                # "/start_auto_quiz — авто-рассылка вопросов по времени\n"
                # "/delete_all — удалить все ответы из базы\n"
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
                # row: (id, user_id, username, full_name, answer_1, answer_2, answer_3, ...)
                user_info = f"{row[3]} (@{row[2]})"
                num_questions = sum(len(block["text"]) for block in questions)
                answers = [f"{i + 1}: {row[4 + i]}" for i in range(num_questions)]
                text += f"{idx}. {user_info}\n" + "\n".join(answers) + "\n\n"
            # Если строк много — отправить частями
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

        @self.router.message(Command("run_block"))
        async def start_block_quiz(message: Message, state: FSMContext):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к запуску блока.")
                return
            data = await state.get_data()
            block_index = data.get("quiz_index", 0)
            if len(message.text.strip().split()) == 2:
                # Позволяет запускать конкретный блок: /run_block 0
                try:
                    block_index = int(message.text.strip().split()[1])
                except ValueError:
                    pass
            if block_index < 0 or block_index >= len(questions):
                await message.answer("Нет такого блока.")
                return

            # Получить всех уникальных пользователей (chat_id, user_id)
            self.cur.execute("SELECT DISTINCT chat_id, user_id FROM answers WHERE chat_id IS NOT NULL")
            users = self.cur.fetchall()
            if not users:
                await message.answer("Нет зарегистрированных участников.")
                return

            count = 0
            for chat_id, user_id in users:
                questions_block = questions[block_index]["text"]
                await state.update_data(block_questions=questions_block, block_step=0, answers=[])
                await self.bot.send_message(chat_id, f"{questions_block[0]}")
                await state.set_state(State.asking)
                count += 1

            await message.answer(f"❗‍INFO❗‍\nБлок #{block_index} запущен для {count} пользователей.")

        @self.router.message(Command("export"))
        async def export_data(message: Message, state: FSMContext):
            await self.admin_export.export_to_sheet(message)

        @self.router.message(Command("download_all_photos"))
        async def download_all_photos_cmd(message: types.Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к этой команде.")
                return
            # Получаем все file_id из базы, где ответы начинаются с photo_file_id:
            self.cur.execute("SELECT * FROM answers")
            rows = self.cur.fetchall()
            columns = [desc[0] for desc in self.cur.description]
            photo_file_ids = []
            for row in rows:
                username = row[3] or "unknown"  # Если username нет — подставить 'unknown'
                for i, col in enumerate(columns):
                    if col.startswith("answer_") and row[i]:
                        if str(row[i]).startswith("photo_file_id:"):
                            photo_file_id = str(row[i]).split(":", 1)[1]
                            photo_file_ids.append((photo_file_id, username))
            # Скачиваем все изображения
            saved = 0
            for file_id, username in photo_file_ids:
                try:
                    await self.download_photo_by_file_id(file_id, username)
                    saved += 1
                except Exception as e:
                    await message.answer(f"Ошибка скачивания: {file_id} — {e}")
            await message.answer(f"Готово! Скачано {saved} изображений.")

# -----------------------------------------------------------------------------------------------------------------

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
        await state.set_state(State.waiting_for_team)

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
        await state.set_state(State.waiting_for_run_quiz)

    async def start_quiz(self, message: types.Message, state: FSMContext):
        index = 0
        block = questions[index]["text"]
        await state.update_data(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            block_questions=block,
            block_step=0,
            answers=[],
            quiz_index=index
        )
        await message.answer(block[0])
        await state.set_state(State.asking)

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
        params.extend([chat_id, user_id])

        self.cur.execute(f"UPDATE answers SET {set_clause}, current_block=? WHERE chat_id=? AND user_id=?",(*answers_padded, index + 1, chat_id, user_id))
        self.conn.commit()
        await state.update_data(quiz_index = index + 1)

    def schedule_all_blocks(self):
        self.job = self.scheduler.add_job(self.timer_block_run, "interval", minutes=1)

    async def timer_block_run(self):
        self.cur.execute("SELECT chat_id, user_id, current_block FROM answers")
        for chat_id, user_id, current_block in self.cur.fetchall():
            await self.try_start_next_block(user_id=user_id, chat_id=chat_id, current_block=current_block)

    async def try_start_next_block(self, chat_id, user_id, current_block):
        now = datetime.now()
        state = FSMContext(self.dp.storage, (chat_id, user_id))
        fsm_state = await state.get_state()
        if fsm_state == State.asking.state:
            return
        for block_index, block in enumerate(questions):
            if (block["time"] <= now) and ((current_block or 0) <= block_index):
                questions_block = block["text"]
                await state.set_data({
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "block_questions": questions_block,
                    "block_step": 0,
                    "answers": [],
                    "quiz_index": block_index,
                })
                await state.set_state(State.asking)
                await self.bot.send_message(chat_id, "Ура! Новый блок вопросов")
                await self.bot.send_message(chat_id, questions_block[0])
                self.cur.execute("UPDATE answers SET current_block=? WHERE chat_id=? AND user_id=?",
                                 (block_index, chat_id, user_id))
                self.conn.commit()
                break

    async def main(self):
        self.scheduler.start()
        await self.dp.start_polling(self.bot)

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

    def _get_all_answers_data(self):
        # Берем все строки из таблицы answers
        self.cur.execute("SELECT * FROM answers")
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
            data = self._get_all_answers_data()
            sheet.clear()  # Чистим лист перед загрузкой
            sheet.update('A1', data)  # Загружаем данные начиная с ячейки A1
            await message.answer("Данные успешно экспортированы в Google Таблицу.")
        except Exception as e:
            await message.answer(f"Произошла ошибка при экспорте: {e}")
            logging.exception("Ошибка при экспорте в Google Sheets")


if __name__ == "__main__":
    tg_bot = InteractiveBot(API_TOKEN)
    asyncio.run(tg_bot.main())