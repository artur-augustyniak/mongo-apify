

import logging
import os
import connexion
import logstash
import logaugment
from connexion.resolver import RestyResolver
from .CRUD import MongoProvider
from flask_cors import CORS
from .swagger import complete_yaml
import tempfile

logger = logging.getLogger(__name__)


def get_apigw_user():
    api_key = connexion.request.headers.get('X-API-Key', None)
    if api_key:
        return api_key.split(":")[0]
    else:
        return "system"


def make_connexion_app(
    api_version,
    host,
    base_path,
    scheme,
    service_name,
    api_file,
    log_level='INFO',
    logstash_host=None,
    logstash_port=0,
    strict_slashes=True
):

    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(complete_yaml(api_file, api_version,
                                host, base_path, scheme).encode())

        loglevel = logging._nameToLevel.get(
            log_level,
            logging.DEBUG
        )
        logging.basicConfig()
        root = logging.getLogger()
        root.setLevel(level=loglevel)
        logaugment.set(logger, service=service_name)

        logger.info("selected loglevel %s" %
                    (logging._levelToName.get(loglevel, "NOSET")))
        if logstash_host is not None and logstash_port > 0:
            LOGSTASH_HANDLER = logstash.UDPLogstashHandler(
                logstash_host, logstash_port, version=1)
            LOGSTASH_HANDLER.setLevel(loglevel)
            root.addHandler(LOGSTASH_HANDLER)
            logger.info("logstash configured %s:%s" %
                        (logstash_host, logstash_port))
        else:
            logger.info("logstash not configured")

        app = connexion.App(__name__, specification_dir="/tmp")
        flask_app = app.app
        flask_app.url_map.strict_slashes = strict_slashes
        CORS(app.app)
        app.add_api(tmp.name, resolver=RestyResolver('api'))
        return app
