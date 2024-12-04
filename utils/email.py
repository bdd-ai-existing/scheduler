import smtplib
from email.message import EmailMessage
import os
from config import settings
from jinja2 import Environment, FileSystemLoader

SMTP_USER = settings.SMTP_USER
SMTP_PASSWORD = settings.SMTP_PASSWORD

# Initialize Jinja2 environment
env = Environment(loader=FileSystemLoader("templates"))

def render_template_with_jinja(template_name, **kwargs):
    """
    Render an email template using Jinja2.
    :param template_name: Template file name.
    :param kwargs: Dynamic data for the template.
    :return: Rendered template as a string.
    """
    template = env.get_template(template_name)

    platform_list = render_platforms_list(kwargs.get("platforms", []))
    
    return template.render(**kwargs, platforms_list=platform_list)

def render_platforms_list(platforms):
    """
    Generate HTML for the platforms list.
    :param platforms: List of platforms with links.
    :return: Rendered HTML as a string.
    """
    platforms_html = ""
    for platform in platforms:
        platforms_html += f"""
        <div class="platform">
            <strong>{platform['platform']}</strong>: 
            <a href="{platform['link']}" target="_blank">Renew Token</a>
        </div>
        """
    return platforms_html

def send_batch_emails(email_data_list):
    """
    Send batch emails using a single SMTP session.

    :param email_data_list: List of email data dictionaries, each with keys:
                            - "user_email"
                            - "subject"
                            - "template_name"
                            - "template_data" (dict with template values)
    :param smtp_user: Gmail address to send the email
    :param smtp_password: Gmail app password
    """
    try:
        # Connect to Gmail SMTP server
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)

            for email_data in email_data_list:
                # Render the email body using Jinja2
                body = render_template_with_jinja(
                    template_name=email_data["template_name"],
                    **email_data["template_data"]
                )

                # Configure the email message
                msg = EmailMessage()
                msg["From"] = SMTP_USER
                msg["To"] = email_data["user_email"]
                msg["Subject"] = email_data["subject"]
                msg.set_content(body, subtype="html")

                # Send the email
                server.send_message(msg)
                print(f"Email sent to {email_data['user_email']}")
    except Exception as e:
        print(f"Batch email sending failed: {e}")
        print(SMTP_USER)
        print(SMTP_PASSWORD)
