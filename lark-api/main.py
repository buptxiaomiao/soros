from flask import Flask

import os
import lark_oapi as lark
from lark_oapi.adapter.flask import *
from lark_oapi.api.application.v6 import P2ApplicationBotMenuV6
from lark_oapi.api.im.v1 import *

app = Flask(__name__)


class Action(object):

    SHUTDOWN_NOW = "shutdown now"

    START_TASK = 'cd /root/github/soros && git pull origin master && ' \
                 'cd bash && bash ./ods.sh && cd ../ts && python l1.py >> /root/log/`date +"%F"`.log'


def create_reply(rep):
    reply = CreateMessageRequest.builder() \
        .receive_id_type("open_id") \
        .request_body(CreateMessageRequestBody.builder()
                      .receive_id('ou_0883cfb95cc4200eb4ee6a5ed06ab47a')
                      .msg_type('text')
                      .content(rep)
                      .build()) \
        .build()
    client.im.v1.message.create(reply)


def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    print(lark.JSON.marshal(data))


def do_bot_menu(data: P2ApplicationBotMenuV6) -> None:
    print(lark.JSON.marshal(data))
    open_id = data.event.operator.operator_id.open_id
    event_key = data.event.event_key
    if open_id != 'ou_0883cfb95cc4200eb4ee6a5ed06ab47a':
        return

    if event_key == '关机':
        create_reply("receive 关机. from ou_088")
        os.system(Action.SHUTDOWN_NOW)

    elif event_key == '执行任务':
        create_reply("receive 执行任务. from ou_088")
        os.system(Action.START_TASK)

    else:
        create_reply("I am healthy.")


def do_customized_event(data: lark.CustomizedEvent) -> None:
    print(lark.JSON.marshal(data))


handler = lark.EventDispatcherHandler.builder(lark.ENCRYPT_KEY, lark.VERIFICATION_TOKEN, lark.LogLevel.DEBUG) \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .register_p1_customized_event("message", do_customized_event) \
    .register_p2_application_bot_menu_v6(do_bot_menu) \
    .build()

client = lark.Client.builder().app_id(lark.APP_ID).app_secret(lark.APP_SECRET).build()


@app.route("/lark/event", methods=["POST", "GET"])
def event():
    resp = handler.do(parse_req())
    return parse_resp(resp)


if __name__ == "__main__":
    print(lark.ENCRYPT_KEY, lark.VERIFICATION_TOKEN)
    app.run(port=7777)
    # gunicorn -w 4 -b 0.0.0.0:8080 main:app


