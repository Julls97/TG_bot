import asyncio
import logging
import sqlite3
import os
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.types import Message

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

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
        "time": datetime.today() + timedelta(hours=9, minutes=55)
    },
    {
        "text": [
            "Сделайте и отправьте креативную фотографию с коллегой с которым чаще всего взаимодействуешь по работе (приветствуется использование ИИ)."
        ],
        "time": datetime.today() + timedelta(hours=12, minutes=30)
    },
    {
        "text": [
            "Какой продукт нашей компании тебе нравится больше всего и почему?\nОпиши, что именно в этом продукте привлекает тебя — будь то функциональность, дизайн, польза для клиентов или что-то ещё. Постарайся раскрыть свои личные впечатления и причины выбора.",
            "С помощью ИИ сгенерируй и направь сюда ответ с нестандартными способами использования продукта, о котором ты писал(а) выше, выходящими за рамки его традиционного применения."
        ],
        "time": datetime.today() + timedelta(hours=14, minutes=00)
    },
    {
        "text": [
            "Если бы вы могли воплотить принципы Agile в образе живого существа или объекта, что бы это было и почему?",
            "Как бы вы переосмыслили одно из ключевых правил Agile, чтобы оно отражало не только гибкость и скорость, но и вдохновение и творческий подход в работе команды?",
            "Расшифруйте ребус из эмодзи и напишите, какое Agile-понятие или практика здесь изображены\n 🐢📅🛠",
        ],
        "time": datetime.today() + timedelta(hours=14, minutes=30)
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
        "time": datetime.today() + timedelta(hours=16, minutes=00)
    },
]

class State(StatesGroup):
    waiting_for_fio = State()
    waiting_for_team = State()
    waiting_for_run_quiz = State()
    asking = State()

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

        self.scheduler = AsyncIOScheduler() # Инициализация планировщика
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
                {answers_cols}
            )
        """)
        self.conn.commit()

    ### 3. Добавьте функцию для отложенной отправки сообщения
    async def send_scheduled_message(self, chat_id, text):
        await self.bot.send_message(chat_id, text)

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
                f"INSERT INTO answers (user_id, chat_id, username, full_name, fio, team) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    user.id,
                    chat_id,
                    user.username or "",
                    user.full_name or "",
                    fio,
                    "",
                )
            )
            self.conn.commit()
            last_row_id = self.cur.lastrowid
            await state.update_data(fio=fio, row_id=last_row_id)

            await message.answer(f"Отлично, {fio}")
            await self.team(message, state)

        @self.router.message(State.waiting_for_team)
        async def error_on_team(message: Message):
            # Если человек отправил текст, а не нажал кнопку
            await message.answer("Пожалуйста, выберите вариант с помощью кнопки!")

        @self.router.callback_query(State.waiting_for_team)
        async def setup_team(callback: types.CallbackQuery, state: FSMContext):
            choice = callback.data.split("_")[1]
            data = await state.get_data()
            row_id = data.get("row_id")
            if row_id:
                self.cur.execute(
                    "UPDATE answers SET team=? WHERE id=?",
                    (choice, row_id)
                )
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

        # @self.router.message(State.asking)
        # async def next_question(message: types.Message, state: FSMContext):
        #     data = await state.get_data()
        #     questions_block = data.get("block_questions", [])
        #     step = data.get("block_step", 0)
        #     answers = data.get("answers", [])
        #
        #     # --- Обработка фото-вопроса (например, 3-й блок, 1-й вопрос)
        #     photo_question_index = (len(questions[0]["text"]) + len(questions[1]["text"]))  # позиция фото-вопроса в общем списке
        #     is_photo_q = step + sum(len(q["text"]) for q in questions[:2]) == photo_question_index
        #
        #     if is_photo_q and message.photo:
        #         photo_id = message.photo[-1].file_id  # берём лучшее качество
        #         answers.append(f"[photo:{photo_id}]")
        #         row_id = data.get("row_id")
        #         # photo_1 соответствует нашему вопросу (можно привязать по step, если фото вопросов несколько)
        #         self.cur.execute("UPDATE answers SET photo=? WHERE id=?", (photo_id, row_id))
        #         self.conn.commit()
        #         step += 1
        #     else:
        #         answers.append(message.text)
        #         step += 1
        #
        #     if step < len(questions_block):
        #         await state.update_data(block_step=step, answers=answers)
        #         await message.answer(questions_block[step])
        #     else:
        #         await self.save_answers(answers, state)
        #         await message.answer("Спасибо за ваши ответы! Они записаны.\nЖди следующих сообщений")
        #         await state.clear()

# -----------------------------------------------------------------------------------------------------------------
        @self.router.message(State.asking)
        async def next_question(message: types.Message, state: FSMContext):
            data = await state.get_data()

            questions_block = data.get("block_questions", [])
            step = data.get("block_step", 0)
            answers = data.get("answers", [])
            answers.append(message.text)
            step += 1

            if step < len(questions_block):
                await message.answer(questions_block[step])
                await state.update_data(block_step=step, answers=answers)
            else:
                await self.save_answers(answers, state)
                await message.answer("Спасибо за ваши ответы! Они записаны.\nЖди следующих сообщений")
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
                result = "\n".join([f"{i+1}. {q}" for i, q in enumerate(block)])
                await message.answer(f"Вопросы к блоку #{idx}\n{result}")
            else:
                await message.answer("Нет такого блока.")

        @self.router.message(Command("run_block"))
        async def start_block_quiz(message: Message, state: FSMContext):
            # if message.from_user.id != ADMIN_ID:
            #     await message.answer("У вас нет доступа к запуску блока.")
            #     return
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

        @self.router.message(Command("remind"))
        async def remind_example(message: Message):
            if message.from_user.id != ADMIN_ID:
                await message.answer("У вас нет доступа к напоминаниям.")
                return
            run_time = datetime(2025, 8, 7, 18, 14, 30)
            await message.answer("Через минуту это сообщение напомнит о себе!")
            # Через минуту придет напоминание
            self.scheduler.add_job(
                self.send_scheduled_message,
                "date",
                run_date=run_time,
                args=[message.chat.id, "Прошла минута: напоминаем о себе! 😉"]
            )

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
        await state.update_data(block_questions=block, block_step=0, block_answers=[], quiz_index=index)
        await message.answer(block[0])
        await state.set_state(State.asking)

    async def save_answers(self, answers, state: FSMContext):
        data = await state.get_data()
        row_id = data.get("row_id")
        index = data.get("quiz_index", 0)  # Получаем индивидуальный индекс

        questions_count = len(questions[index]["text"])
        answers_padded = answers + [""] * (questions_count - len(answers))

        if row_id:
            set_clause = ', '.join([f"answer_{i + 1}=?" for i in range(questions_count)])
            self.cur.execute(f"UPDATE answers SET {set_clause} WHERE id=?",(*answers_padded, row_id))
            self.conn.commit()

        index += 1
        await state.update_data(quiz_index=index)  # Сохраняем обновлённый индекс

        # # Логика перехода или завершения
        # if index < len(questions):
        #     block = questions[index]["text"]
        #     await state.update_data(block_questions=block, step=0, answers=[])
        #     # Отправляем следующий блок вопросов
        #     await self.bot.send_message(
        #         chat_id=row_id,  # Или используйте message.from_user.id или аналогично, если есть access к message
        #         text=block[0]
        #     )
        #     await state.set_state(State.asking)
        # else:
        #     # Закончились вопросы
        #     await self.bot.send_message(
        #         chat_id=row_id,
        #         text="На этом вопросы закончились! Спасибо за участие."
        #     )
        #     await state.clear()

    def get_all_answers(self):
        self.cur.execute("SELECT * FROM answers")
        return self.cur.fetchall()

    async def main(self):
        self.scheduler.start()
        await self.dp.start_polling(self.bot)


if __name__ == "__main__":
    bot1 = InteractiveBot(API_TOKEN)
    asyncio.run(bot1.main())