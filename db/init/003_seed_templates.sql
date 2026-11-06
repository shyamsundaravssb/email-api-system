INSERT INTO templates (
    template_id,
    subject_template,
    html_template,
    text_template,
    required_variables
)
VALUES

-- 1. OTP Email
(
    'otp_email',
    'Your One-Time Password – Grasshopper',

    $html$
    <!DOCTYPE html>
    <html>
    <body>
        <h2>Grasshopper</h2>

        <p>Hello {{ name }},</p>

        <p>Your one-time password (OTP) is:</p>

        <h2>{{ otp }}</h2>

        <p>Please use this code to complete your verification.</p>

        <p>If you did not request this code, you can safely
        ignore this email.</p>

        <p>Regards,<br>Team Grasshopper</p>
    </body>
    </html>
    $html$,

    $text$
Hello {{ name }},

Your one-time password (OTP) is: {{ otp }}

Please use this code to complete your verification.

If you did not request this code, you can safely ignore this email.

Regards,
Team Grasshopper
    $text$,

    ARRAY['name', 'otp']::TEXT[]
),

-- 2. Welcome Email
(
    'welcome_email',
    'Welcome to Grasshopper!',

    $html$
    <!DOCTYPE html>
    <html>
    <body>
        <h2>Welcome to Grasshopper!</h2>

        <p>Hello {{ name }},</p>

        <p>We're glad to have you with us!</p>

        <p>Your account is ready, and you can now get started
        with Grasshopper.</p>

        <p>Thank you for choosing us. We look forward to
        being part of your journey.</p>

        <p>Regards,<br>Team Grasshopper</p>
    </body>
    </html>
    $html$,

    $text$
Hello {{ name }},

We're glad to have you with us!

Your account is ready, and you can now get started
with Grasshopper.

Thank you for choosing us. We look forward to being
part of your journey.

Regards,
Team Grasshopper
    $text$,

    ARRAY['name']::TEXT[]
),

-- 3. Password Reset Email
(
    'password_reset',
    'Password Reset Request – Grasshopper',

    $html$
    <!DOCTYPE html>
    <html>
    <body>
        <h2>Password Reset</h2>

        <p>Hello {{ name }},</p>

        <p>We received a request to reset your Grasshopper
        account password.</p>

        <p>Click the link below to proceed:</p>

        <p>
            <a href="{{ reset_link }}">Reset Your Password</a>
        </p>

        <p>If you did not request a password reset, you can
        safely ignore this email.</p>

        <p>Regards,<br>Team Grasshopper</p>
    </body>
    </html>
    $html$,

    $text$
Hello {{ name }},

We received a request to reset your Grasshopper
account password.

Use the following link to reset your password:

{{ reset_link }}

If you did not request a password reset, you can
safely ignore this email.

Regards,
Team Grasshopper
    $text$,

    ARRAY['name', 'reset_link']::TEXT[]
)

ON CONFLICT (template_id) DO NOTHING;

