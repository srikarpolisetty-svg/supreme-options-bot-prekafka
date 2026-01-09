import time
from datetime import datetime
import smtplib
from email.message import EmailMessage


def send_text(message: str):
    msg = EmailMessage()
    msg.set_content(message)
    msg["From"] = "srikarpolisetty@gmail.com"
    msg["To"] = "srikarpolisetty@gmail.com"
    msg["Subject"] = "Trading Alert"

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login("srikarpolisetty@gmail.com", "qxbd zauk zbbz aoqp")
            smtp.send_message(msg)
    except Exception as e:
        print("ERROR in send_text:", repr(e))






