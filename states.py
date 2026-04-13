from aiogram.fsm.state import State, StatesGroup


class BookingStates(StatesGroup):
    choosing_product = State()
    choosing_pay_method = State()
    choosing_studio_mode = State()
    choosing_tariff = State()
    choosing_tariff_date = State()
    choosing_date = State()
    choosing_slot = State()
    entering_brief = State()
    entering_contacts = State()
    waiting_payment = State()
    awaiting_payment_confirm = State()
    reschedule_pick_date = State()
    reschedule_pick_slot = State()


class AdminStates(StatesGroup):
    action_date = State()
    slot_time_input = State()
    cancel_booking_input = State()
    price_wait_value = State()
    toggle_slots_pick = State()
    wait_setting_text = State()
    equipment_photo_wait = State()
    directions_video_wait = State()
    ui_photo_wait = State()

