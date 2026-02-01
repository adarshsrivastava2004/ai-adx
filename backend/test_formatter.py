from formatter import format_response

def run_test(name: str, user_query: str, system_json: dict):
    print("\n" + "=" * 60)
    print(f"TEST CASE: {name}")
    print("=" * 60)

    print("\nUser Query:")
    print(user_query)

    print("\nSystem JSON:")
    print(system_json)

    print("\nFormatter Output:")
    print(format_response(user_query, system_json))
    print("\n")


if __name__ == "__main__":

    # -----------------------
    # TEST 1: Normal Chat
    # -----------------------
    run_test(
        name="Chat response",
        user_query="Hello, what can you do?",
        system_json={
            "tool": "chat",
            "reply": "This question does not require database access."
        }
    )

    # -----------------------
    # TEST 2: ADX Success (few rows)
    # -----------------------
    run_test(
        name="ADX success (few rows)",
        user_query="Top 3 states by average crop damage",
        system_json={
            "tool": "adx",
            "rows": 3,
            "data": [
                {"State": "TEXAS", "AvgCropDamage": 123456},
                {"State": "IOWA", "AvgCropDamage": 98765},
                {"State": "NEBRASKA", "AvgCropDamage": 87654}
            ]
        }
    )

    # -----------------------
    # TEST 3: ADX Success (many rows)
    # -----------------------
    run_test(
        name="ADX success (many rows)",
        user_query="Average crop damage by state",
        system_json={
            "tool": "adx",
            "rows": 10,
            "data": [
                {"State": "STATE1", "AvgCropDamage": 1000},
                {"State": "STATE2", "AvgCropDamage": 1200},
                {"State": "STATE3", "AvgCropDamage": 1100},
                {"State": "STATE4", "AvgCropDamage": 1300},
                {"State": "STATE5", "AvgCropDamage": 1250},
                {"State": "STATE6", "AvgCropDamage": 900},
                {"State": "STATE7", "AvgCropDamage": 1400},
                {"State": "STATE8", "AvgCropDamage": 1500},
                {"State": "STATE9", "AvgCropDamage": 1600},
                {"State": "STATE10", "AvgCropDamage": 1700},
            ]
        }
    )

    # -----------------------
    # TEST 4: Ambiguous query
    # -----------------------
    run_test(
        name="Ambiguous query",
        user_query="Show worst events",
        system_json={
            "tool": "adx",
            "error": "Ambiguous or unsupported query goal"
        }
    )

    # -----------------------
    # TEST 5: Validation error
    # -----------------------
    run_test(
        name="Validation error",
        user_query="Total damage for florida",
        system_json={
            "tool": "adx",
            "error": "State values must be uppercase"
        }
    )
