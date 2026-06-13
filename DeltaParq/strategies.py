from typing import Callable

STRATEGIES = {
    "daily_snapshot": lambda x: x,
    "first_of_the_month": lambda x: 1,
    "progressive": lambda x: x - 1 if x != 1 else 1,
    "bst": lambda x: 1 if x == 1 else x // 2,
    "optimized_bst": lambda x: 1 if x == 1 else (1 if x == 16 else (x //2 if x < 16 else ((x-16)//2 + 16))),
    "optimized_back_to_1_bst": lambda x: 1 if x == 1 else (x //2 if x < 17 else ((x-16)//2 + 16)),
    "trying_to_hard_bst": lambda x: 1 if x == 1 else (x // 2 if x < 8 else (1 if x == 8 else ((x-8)//2 + 8) if x < 16 else (1 if x == 16 else ((x-16)//2 + 16 if x < 24 else (1 if x == 24 else ((x-24)//2+24)))))),
    "without_back_to_1_bst": lambda x: 1 if x == 1 else (x // 2 if x < 9 else ((x-8)//2 + 8) if x < 17 else ((x-16)//2 + 16 if x < 25 else ((x-24)//2+24)))
}

def get_strategy(strategy: str) -> Callable[[int], int]:
    try:
        return STRATEGIES[strategy]
    except KeyError:
        raise Exception(f"Strategy \"{strategy}\" is not an available strategy.") from None
