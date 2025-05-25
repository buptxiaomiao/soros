import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MailTool:
    smtp_server = "smtp.sina.com"
    smtp_port = 0
    user = os.getenv("MAIL_FROM")
    password = os.getenv("MAIL_PASSWORD")
    mail_to = os.getenv("MAIL_TO")

    @classmethod
    def send(cls, subject, df_tuple_list):
        # 生成美化后的HTML内容[6,8](@ref)
        html_content = dataframe_tuple_list_to_styled_html(df_tuple_list)

        # 构建邮件对象[2,7](@ref)
        msg = MIMEMultipart('alternative')

        # msg = MIMEText("邮件正文内容", "plain", "utf-8")
        msg["From"] = cls.user
        msg["To"] = cls.mail_to
        msg["Subject"] = subject

        # 附加HTML内容
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        with smtplib.SMTP_SSL(cls.smtp_server, cls.smtp_port) as server:
            res_login = server.login(cls.user, cls.password)  # 需使用邮箱提供的授权码，非登录密码[7](@ref)
            print(f"stmp.login={res_login}")
            res_send = server.sendmail(msg["From"], msg["To"], msg.as_string())
            for t in df_tuple_list:
                print(f"df={t[0].shape}, df.columns={t[0].columns}. title={t[1] if len(t)>1 else ''}")
            print(f"stmp.send to {msg['To']} finish. res_send={res_send}")

def dataframe_tuple_list_to_styled_html(tuple_list):
    """
    将DataFrame转换为带CSS样式的HTML表格
    :param tuple_list:
        0: pandas DataFrame对象
        1: title: 表格标题
    :return: 格式化后的HTML字符串
    """

    # 定义CSS样式模板[3,8](@ref)
    css_style = """<style>
          .table-wrapper {
            max-width: 100%;
            overflow-x: auto;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
          }
          .dataframe {
            border-collapse: collapse;
            border: 2px solid #2c3e50;
            width: max-content;
          }
          .dataframe th {
            background-color: #2c3e50;
            color: white;
            padding: 14px;
            position: sticky;
            left: 0;
            z-index: 2;
          }
          .dataframe td {
            padding: 14px;
            border: 1px solid #dee2e6;
            background: white;
            position: relative;
          }
          /* 固定列样式 */
          .dataframe td:nth-child(1), .dataframe th:nth-child(1),
          .dataframe td:nth-child(2), .dataframe th:nth-child(2) {
            position: sticky;
            left: 0;
            z-index: 1;
            background: #f8f9fa;
            border-right: 2px solid #2c3e50;  /* 新增分隔线 */
          }
          .dataframe td:nth-child(2), .dataframe th:nth-child(2) {
            left: 120px;
          }
        </style>
        """
    body = ''.join([f"""
        {f'<h3 style="color:#2c3e50">{t[1]}</h3>' if len(t) > 1 else ''}
        {t[0].to_html(index=False, border=0, classes='dataframe')}
    """ for t in tuple_list])

    # 构建完整HTML结构
    html_content = f"""
    <!DOCTYPE html>
    <html>
        <head>{css_style}</head>
        <body>{body}</body>
    </html>
    """
    return html_content

if __name__ == '__main__':

    from api.stock_list_rt import StockListRT
    res = StockListRT.realtime_list()
    df = res[0]
    tuple_list = [
        (df, '测试TITLE')
    ]
    MailTool.send("2025-05-25主题", tuple_list)

