from flask import Flask, render_template, request
import mysql.connector
import yaml

app = Flask(__name__)

def load_secrets():
    with open('secrets.yml', 'r') as f:
        secrets = yaml.load(f, Loader=yaml.FullLoader)
    return secrets

@app.route('/', methods=['GET', 'POST'])
def homepage():
    secret_info = load_secrets()
    context = {
        'edit_count_min': 0,
    }

    if request.method == 'POST':
        database = request.form.get('database')
        edit_count_min = request.form.get('edit-count-min')
        edit_count_max = request.form.get('edit-count-max')

        context['database'] = database
        context['edit_count_min'] = edit_count_min
        context['edit_count_max'] = edit_count_max
        print(context)

        db_connection = mysql.connector.connect(
            host=f"{database}.analytics.db.svc.wikimedia.cloud",
            user=secret_info['toolforge_user'],
            password=secret_info['toolforge_password'],
            database=f"{database}_p"
        )

        # Connecting to the replicas is a pain locally, so use mock data locally
        # and a real database connection & query if we're on Toolforge.
        if app.config['FLASK_ENV'] == 'development':
            pass
        elif app.config['FLASK_ENV'] == 'production':
            cursor = db_connection.cursor()

            cursor.execute(f"""
                SELECT DISTINCT
                    actor.actor_name AS user_name,
                    user.user_editcount
                FROM user
                JOIN actor ON actor.actor_user = user.user_id
                JOIN revision ON revision.rev_actor = actor.actor_id
                WHERE revision.rev_timestamp > 20251013000000
                AND user.user_editcount BETWEEN {edit_count_min} AND {edit_count_max}
                AND user.user_is_temp = False
                ORDER BY user.user_editcount DESC;
            """)

            user_list = cursor.fetchall()

        context['user_list'] = user_list

    return render_template('index.html', **context)


if __name__ == '__main__':
    app.run()
