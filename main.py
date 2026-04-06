"""
Entry point — create Flask app, register blueprints, run.
"""

from flask import Flask
import config
from routes import bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app


if __name__ == "__main__":
    app = create_app()
    print(f"Starting Invoice → GRN agent on port {config.PORT}")
    app.run(host="0.0.0.0", port=config.PORT, debug=True)
