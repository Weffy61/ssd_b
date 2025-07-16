#!/bin/bash
cd /opt/ssd_b
source venv/bin/activate
export DJANGO_SETTINGS_MODULE=ssn_dob_bot.settings
python manage.py migrate