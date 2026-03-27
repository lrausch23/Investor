try:
    from .streamlit_app import main
except ImportError:
    from src.regime.streamlit_app import main


if __name__ == "__main__":
    main()
