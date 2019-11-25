import connexion

from flask_cors import CORS
from flask import request
from flask import g

import logging
logging.basicConfig(level=logging.INFO)

app = connexion.App(__name__, specification_dir='./openapi/')
app.add_api('openapi.yaml', arguments={'title': 'travelinfo'})
CORS(app.app)

flaskapp = app.app


@app.app.before_request
def before_request():
    g.user = ''
    g.ip = ''


@app.app.after_request
def after_request_callback(response):
    logger = logging.getLogger('auditlog')
    auditlog_list = list(filter(None, [
        "Request Url: {}".format(request.url),
        "IP: {}".format(g.ip),
        "User-Agent: {}".format(request.headers.get('User-Agent')),
        "Response status: {}".format(response.status),
        "UPN: {}".format(g.user)
    ]))

    logger.info(' | '.join(auditlog_list))

    return response
