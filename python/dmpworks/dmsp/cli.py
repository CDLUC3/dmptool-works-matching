from cyclopts import App

from dmpworks.dmsp.related_works import app as related_works_app

app = App(name="dmsp", help="Utilities for the DMSP database.")

app.command(related_works_app)


if __name__ == "__main__":
    app()
