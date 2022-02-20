import logging
import os
import connexion
import logaugment
from connexion.resolver import RestyResolver
from .CRUD import MongoProvider
from flask_cors import CORS
from .swagger import complete_yaml
import tempfile

logger = logging.getLogger(__name__)


def get_header_from_request(name: str):
    return connexion.request.headers.get(name, None)


def make_connexion_app(
    api_version,
    host,
    base_path,
    scheme,
    service_name,
    swagger_dir,
    log_level="INFO",
    strict_slashes=True,
):

    loglevel = logging._nameToLevel.get(log_level, logging.DEBUG)
    logging.basicConfig()
    root = logging.getLogger()
    root.setLevel(level=loglevel)
    logaugment.set(logger, service=service_name)
    logger.info("selected loglevel %s" % (logging._levelToName.get(loglevel, "NOSET")))

    yaml_paths = []

    for file in os.listdir(swagger_dir):
        if file.endswith(".yaml"):
            def_path = os.path.join(swagger_dir, file)
            yaml_paths.append(def_path)
            logger.info("Loading api definition from %s" % (def_path))

    app = connexion.App(__name__, specification_dir="/tmp")
    flask_app = app.app
    flask_app.url_map.strict_slashes = strict_slashes
    CORS(app.app)
    for yaml_path in yaml_paths:
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(
                complete_yaml(yaml_path, api_version, host, base_path, scheme).encode()
            )
            app.add_api(tmp.name, resolver=RestyResolver("api"))
    return app
