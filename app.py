from datetime import datetime, timedelta
from flask import Flask, render_template, request
import mysql.connector
import yaml

app = Flask(__name__)

def load_secrets():
    with open('secrets.yml', 'r') as f:
        secrets = yaml.load(f, Loader=yaml.FullLoader)
    return secrets

def date_as_mw_timestamp(date_string):
    return date_string.strftime("%Y%m%d") + "000000"

@app.route('/', methods=['GET', 'POST'])
def homepage():
    secret_info = load_secrets()
    context = {}

    if request.method == 'POST':
        database = request.form.get('database')
        edit_count_min = request.form.get('edit-count-min')
        edit_count_max = request.form.get('edit-count-max')
        account_age_min = request.form.get('account-age-min')
        account_age_max = request.form.get('account-age-max')

        context['database'] = database
        context['edit_count_min'] = edit_count_min
        context['edit_count_max'] = edit_count_max
        context['account_age_min'] = account_age_min
        context['account_age_max'] = account_age_max

        # Requires SSH tunnel per
        # https://wikitech.wikimedia.org/wiki/Help:Wiki_Replicas#Connecting_to_the_database_replicas_from_your_own_computer
        if app.config['DEBUG']:

            db_connection = mysql.connector.connect(
                host="127.0.0.1",
                user=secret_info['toolforge_user'],
                password=secret_info['toolforge_password'],
                database=f"{database}_p"
            )

        elif not app.config['DEBUG']:

            db_connection = mysql.connector.connect(
                host=f"{database}.analytics.db.svc.wikimedia.cloud",
                user=secret_info['toolforge_user'],
                password=secret_info['toolforge_password'],
                database=f"{database}_p"
            )

        edit_count_min_query = ""
        if edit_count_min:
            edit_count_min_query = f"AND user.user_editcount > {edit_count_min}"

        edit_count_max_query = ""
        if edit_count_max:
            edit_count_max_query = f"AND user.user_editcount < {edit_count_max}"

        now = datetime.now()

        account_age_min_query = ""
        if account_age_min:
            account_min_date = now - timedelta(days=int(account_age_min))
            account_min_date_mw = date_as_mw_timestamp(account_min_date)
            account_age_min_query = f"AND user.user_registration < {account_min_date_mw}"

        account_age_max_query = ""
        if account_age_max:
            account_max_date = now - timedelta(days=int(account_age_max))
            account_max_date_mw = date_as_mw_timestamp(account_max_date)
            account_age_max_query = f"AND user.user_registration > {account_max_date_mw}"

        one_week_ago = now - timedelta(days=7)
        one_week_ago_timestamp = date_as_mw_timestamp(one_week_ago)

        query = f"""
            SELECT DISTINCT
                actor.actor_name AS user_name,
                user.user_editcount
            FROM user
            JOIN actor ON actor.actor_user = user.user_id
            JOIN revision ON revision.rev_actor = actor.actor_id
            WHERE revision.rev_timestamp > {one_week_ago_timestamp}
            AND user.user_is_temp = False
            {edit_count_min_query}
            {edit_count_max_query}
            {account_age_min_query}
            {account_age_max_query}
            ORDER BY user.user_editcount DESC;
        """

        cursor = db_connection.cursor()

        cursor.execute(query)

        user_list = cursor.fetchall()

        # TODO: Return a CSV

        context['user_list'] = user_list
        context['query'] = query

    return render_template('index.html', **context)


if __name__ == '__main__':
    app.run()
