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

## CRITICAL SYSTEM INTEGRITY RULE
NEVER make changes without thoroughly evaluating how they will impact the entire application flow. Before making ANY code changes:

1. **Analyze Dependencies**: Understand how the change affects other parts of the system
2. **Trace Data Flow**: Follow how data moves through the application end-to-end  
3. **Check Related Functions**: Examine all functions that use the same tables/fields
4. **Consider Side Effects**: Evaluate potential unintended consequences
5. **Ask for Clarification**: If uncertain about impacts, ask the user before proceeding

Making isolated fixes that break other parts of the application is unacceptable. System-wide thinking is required for all changes.