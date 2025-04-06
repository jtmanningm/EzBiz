# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Run app: `streamlit run main.py`
- Run specific test: `pytest database/test_connection.py -v`
- Lint code: `ruff check .`
- Format code: `black .`

## Code Style

- **Imports**: Group by standard library, third-party, local; alphabetize within groups
- **Typing**: Use type hints for function parameters and return values
- **Naming**: snake_case for functions/variables, PascalCase for classes, ALL_CAPS for constants
- **Models**: Use @dataclass for model classes with to_dict/from_dict methods
- **SQL**: Use uppercase for SQL keywords, parameterized queries with placeholders
- **Error handling**: Wrap database operations in try/except blocks, provide meaningful error messages
- **Documentation**: Use docstrings for all public functions and classes
- **UI Components**: Group related UI elements in st.containers() for better organization