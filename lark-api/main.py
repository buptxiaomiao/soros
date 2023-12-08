from flask import Flask

import lark_oapi as lark
from lark_oapi.adapter.flask import *
from lark_oapi.api.application.v6 import P2ApplicationBotMenuV6
from lark_oapi.api.im.v1 import *

app = Flask(__name__)


def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    print(lark.JSON.marshal(data))
    if data.event:
        user_id = data.event.sender.sender_id.user_id
        content = data.event.message.content
        if user_id == '812a23gf' and '关机' in content:
            print("receive 关机. from 812a23gf")
            import os
            os.system("whoami && uptime")


def do_bot_menu(data: P2ApplicationBotMenuV6) -> None:
    print(lark.JSON.marshal(data))


def do_customized_event(data: lark.CustomizedEvent) -> None:
    print(lark.JSON.marshal(data))


handler = lark.EventDispatcherHandler.builder(lark.ENCRYPT_KEY, lark.VERIFICATION_TOKEN, lark.LogLevel.DEBUG) \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .register_p1_customized_event("message", do_customized_event) \
    .register_p2_application_bot_menu_v6(do_bot_menu) \
    .build()


@app.route("/lark/event", methods=["POST", "GET"])
def event():
    resp = handler.do(parse_req())
    return parse_resp(resp)


if __name__ == "__main__":
    print(lark.ENCRYPT_KEY, lark.VERIFICATION_TOKEN)
    app.run(port=7777)
    # gunicorn -w 4 -b 0.0.0.0:8080 main:app


