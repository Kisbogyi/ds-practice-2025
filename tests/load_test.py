import gevent
import locust


class UsersTest(locust.SequentialTaskSet):
    @locust.task
    def api_get_task(self):
        self.client.get("/", name="GET /")


class SituationTest(locust.HttpUser):
    task_set = UsersTest
    min_wait = 1000
    max_wait = 2000
    host = "https://example.com"


def test__your_pytest_example():
    env = locust.env.Environment(user_classes=[SituationTest])
    env.create_local_runner()
    gevent.spawn(locust.stats.stats_history, env.runner)
    env.runner.start(1, spawn_rate=1)
    gevent.spawn_later(10, lambda: env.runner.quit())
    env.runner.greenlet.join()

    assert env.stats.total.avg_response_time < 60
    assert env.stats.total.num_failures == 0
    assert env.stats.total.get_response_time_percentile(0.95) < 100
