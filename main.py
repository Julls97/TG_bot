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

# ----------------- Состояния -------------------
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
            if current_block >= len(questions) - 1 and hasattr(self, 'job'):  # Если все завершили, отменяем задачу
                self.job.remove()
                logging.info("Все блоки пройдены, задача планировщика остановлена.")
            else:
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


if __name__ == "__main__":
    tg_bot = InteractiveBot(API_TOKEN)
    asyncio.run(tg_bot.main())