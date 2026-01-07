import smtplib
EMAIL_HOST="smtp.gmail.com"
EMAIL_PORT=587
EMAIL_USER="angad.gadre@stjoseph.com"
EMAIL_PASSWORD="lwwejbvzzagykrfz"

try:
    smtp_host = EMAIL_HOST
    smtp_port = EMAIL_PORT  # or the port you're using
    smtp_user = EMAIL_USER
    smtp_password = EMAIL_PASSWORD

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.ehlo()
    if smtp_user and smtp_password:
        server.starttls()
        server.login(smtp_user, smtp_password)
        print("SMTP connection successful")
        from_addr = "angad.gadre@stjoseph.com"
        to_addrs = ["angad.gadre@stjoseph.com"]
        msg = """\
        Weekly Analytics Report for torontolife
        Date: July 28 - August 03, 2025
        Pageviews: 610454
        Sessions: 365254
        Engaged Minutes: 105000
        Avg. Engagement Time: 1:30
        Pageviews change vs baseline: 5.2% (normal)
        """
    server.sendmail(from_addr, to_addrs, msg)
    server.quit()
except Exception as e:
    print(f"SMTP connection failed: {e}")

try:
    from premailer import transform  # type: ignore
except ImportError:
    print("error premailer not installed, HTML will not be inlined")
    transform = None  # type: ignore