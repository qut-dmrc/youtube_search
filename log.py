import logging
import socket
import threading
from logging.handlers import RotatingFileHandler
from traceback import format_exc
from config import cfg

from requests import post

_initalised = False
_LOGGER_NAME = 'LegitLogger'


class CountsHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        self.counts = {
        }
        self._countLock = threading.Lock()

        super(CountsHandler, self).__init__(level)

    def emit(self, record):
        self._countLock.acquire()
        self.counts[record.levelname] = self.counts.get(record.levelname, 0) + 1
        self._countLock.release()
        pass

    def get_counts(self):
        return self.counts

    def reset_counts(self):
        self.counts = {}


def getLogger():
    return logging.getLogger(_LOGGER_NAME)


logger = getLogger()


def setup_logging(log_file_name=None, verbose=False, interactive_only=False):
    global _initalised
    if _initalised:
        return logging.getLogger(_LOGGER_NAME)

    if not verbose:
        # Quieten other loggers down a bit (particularly requests and google api client)
        for logger_str in logging.Logger.manager.loggerDict:
            try:
                logging.getLogger(logger_str).setLevel(logging.WARNING)

            except:
                pass

    logFormatter = logging.Formatter(
        "%(asctime)s [%(filename)-20.20s:%(lineno)-4.4s - %(funcName)-20.20s() [%(threadName)-12.12s] [%(levelname)-8.8s]  %(message).5000s")
    logger = logging.getLogger(_LOGGER_NAME)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

    # Add logger to count number of errors
    countsHandler = CountsHandler()
    logger.addHandler(countsHandler)

    if verbose:
        consoleHandler.setLevel(logging.DEBUG)
    else:
        consoleHandler.setLevel(logging.INFO)

    if not interactive_only and log_file_name:
        fileHandler = RotatingFileHandler(log_file_name, maxBytes=20000000, backupCount=20, encoding="UTF-8")
        fileHandler.setFormatter(logFormatter)

        if verbose:
            fileHandler.setLevel(logging.DEBUG)
        else:
            fileHandler.setLevel(logging.INFO)

        logger.addHandler(fileHandler)

    # Add

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    _initalised = True

    return logger


run_summary = {
    'summary_log_messages': [],
    'summary_counts': {},
}


def init_run_summary():
    global run_summary

    run_summary = {
        'summary_log_messages': [],
        'summary_counts': {},
    }


def increment_run_summary(variable_name, value=1):
    global run_summary
    run_summary['summary_counts'][variable_name] = run_summary['summary_counts'].get(variable_name, 0) + value


def log_run_summary(summary_msg, module_name=None):
    global run_summary
    if module_name:
        summary_msg = "[{}] {}\n".format(module_name, summary_msg)
    run_summary['summary_log_messages'].append(summary_msg)
    logger.info(summary_msg)


def get_log_summary(reset_summary=False):
    global run_summary

    message_body = ""

    for line in run_summary['summary_log_messages']:
        message_body += str(line) + "\n"

    for key, value in run_summary['summary_counts'].items():
        message_body += "{key}: {value}\n".format(key=key, value=value)

    logger = getLogger()

    for handlerobj in logger.handlers:
        if isinstance(handlerobj, CountsHandler):
            counts = handlerobj.get_counts()
            message_body += "\n\nLog messages:\n"
            for key, value in counts.items():
                message_body += "{key} messages: {value}\n".format(key=key, value=value)
            if reset_summary:
                handlerobj.reset_counts()

    if reset_summary:
        run_summary = {
            'summary_log_messages': [],
            'summary_counts': {},
        }
    return message_body


def print_run_summary(subject, log_file=None, reset_summary=True, send_email=False):
    message_body = get_log_summary(reset_summary=reset_summary)

    logger.info('\n\n')
    logger.info('----------------------')
    logger.info(subject)
    logger.info(message_body)
    logger.info('----------------------')

    if send_email:
        send_update_mail(subject, message_body)

    if reset_summary:
        init_run_summary() # Not sure why the run summary is not being reset in the get_log_summary() function.


def send_update_mail(subject, message):
    try:
        hostname = socket.gethostname()
        full_address = socket.gethostbyname_ex(hostname)
        send_mail({
            "subject": subject,
            "text": str(message) + "\n" +
                    "From {host}.".format(
                        host=full_address
                    )
        }
        )
    except Exception as e:
        logger.exception("Unable to send error mail: {}".format(e), exc_info=True)


def send_exception(module_name=None, message=None, message_body=None):
    if message:
        logger.exception(message, exc_info=True)
    if message_body:
        message = message_body[:2500]
    try:
        hostname = socket.gethostname()
        full_address = socket.gethostbyname_ex(hostname)
        subject = "[{}] Unexpected Error: {}".format(module_name, message)
        send_mail({
            "subject": subject,
            "text":
                "Unexpected Error, please check your instance {host}.\n{message}\n{traceback}".format(
                    host=full_address,
                    message=message_body,
                    traceback=format_exc()[:2500],
                )
        }
        )
    except Exception as e:
        logger.error("Unable to send error mail: {}".format(e))


def send_mail(message_dict):
    if cfg['mailgun']:
        api_base_url = cfg['mailgun']['mailgun_api_base_url'] + '/messages'

        auth = ('api', cfg['mailgun']['mailgun_api_key'])

        data = {
            "from": "Legit-Platforms <%s>" % cfg['mailgun']['mailgun_default_smtp_login'],
            "to": cfg['mailgun']['email_to_notify']
        }

        data.update(message_dict)
        logger.info("Sent error email to {}.".format(cfg['mailgun']['email_to_notify']))

        return post(api_base_url, auth=auth, data=data)
    else:
        logger.info("Not sending email - mailgun is not configured.")
        return None