import smtplib
from email.message import EmailMessage

def send_email(sender_email, app_specific_password, recipient_email, subject, body):
    """
    Send an email using iCloud's SMTP server.
    """
    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL('smtp.mail.me.com', 465) as smtp:
            smtp.login(sender_email, app_specific_password)
            smtp.send_message(msg)
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
