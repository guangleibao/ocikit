"""Kafka configuration and Java bridge shared by the stream consumer tools.

Keep this module beside runStreamConsumerDemo.sh and getStreamConsumerDashboard.sh.
"""
import base64
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
from types import SimpleNamespace


def kafka_config(args, bootstrap_servers, instance_name):
    return {'bootstrap.servers': bootstrap_servers, 'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN', 'sasl.username': args.kafka_username,
            'sasl.password': os.environ['OCI_KAFKA_AUTH_TOKEN'],
            'group.id': args.group_name, 'client.id': instance_name,
            'group.protocol': 'classic', 'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'enable.auto.commit': False, 'enable.auto.offset.store': False,
            'auto.offset.reset': 'earliest' if args.cursor_type == 'TRIM_HORIZON' else 'latest',
            'allow.auto.create.topics': False, 'max.partition.fetch.bytes': 1024 * 1024,
            'fetch.max.bytes': 1024 * 1024, 'queued.min.messages': 1,
            'queued.max.messages.kbytes': 1024, 'fetch.queue.backoff.ms': 1000,
            'fetch.error.backoff.ms': 1000, 'retry.backoff.ms': 1000,
            'retry.backoff.max.ms': 10000,
            'socket.connection.setup.timeout.ms': max(1000, int(args.connect_timeout * 1000)),
            'socket.timeout.ms': max(1000, int(args.request_timeout * 1000))}


class JavaKafkaRecord:
    """Adapt a Java consumer record to the shared Kafka processing loop."""
    def __init__(self, data):
        self.data = data

    def error(self):
        return None

    def topic(self):
        return self.data['topic']

    def partition(self):
        return self.data['partition']

    def offset(self):
        return self.data['offset']

    def key(self):
        return base64.b64decode(self.data['key']) if self.data['key'] is not None else None

    def value(self):
        return base64.b64decode(self.data['value']) if self.data['value'] is not None else None


JAVA_KAFKA = SimpleNamespace(KafkaException=RuntimeError,
    TopicPartition=lambda topic, partition, offset: SimpleNamespace(topic=topic, partition=partition, offset=offset))


class JavaKafkaClient:
    """One native Java KafkaConsumer per worker; JSON pipes carry commands only."""
    def __init__(self, args, bootstrap_servers, instance_name, *, stderr=None, on_process=None):
        root = Path(os.environ.get('OCIKIT_DIR', '.'))
        jar = root / 'kafka-java' / 'target' / 'stream-consumer.jar'
        if not shutil.which('java') or not jar.is_file():
            raise RuntimeError('Java Kafka client missing; run prepareOL.sh/prepareDebian.sh '
                               'or build with mvn -f kafka-java/pom.xml package (JDK 17+).')
        self.timeout = args.request_timeout + args.connect_timeout + args.poll_interval + 10
        self.process = subprocess.Popen(['java', '-jar', str(jar)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1, start_new_session=True)
        try:
            if on_process is not None:
                on_process(self.process)
            self._request('init', bootstrap=bootstrap_servers, username=args.kafka_username,
                stream_pool_id=args.stream_pool_id,
                group=args.group_name, instance=instance_name, batch_size=args.batch_size,
                cursor_type=args.cursor_type, timestamp_ms=int(args.time.timestamp() * 1000) if args.time else None,
                connect_timeout_ms=max(1000, int(args.connect_timeout * 1000)),
                request_timeout_ms=max(1000, int(args.request_timeout * 1000)), debug=args.debug)
        except BaseException:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process.stdin.close()
            self.process.stdout.close()
            raise

    def _request(self, operation, **values):
        self.process.stdin.write(json.dumps(dict(op=operation, **values)) + '\n')
        self.process.stdin.flush()
        if not select.select([self.process.stdout], [], [], self.timeout)[0]:
            raise RuntimeError('Java Kafka client timed out responding to ' + operation)
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError('Java Kafka client exited during ' + operation + '; see stderr')
        response = json.loads(line)
        if 'error' in response:
            raise RuntimeError(response['error'])
        return response

    def subscribe(self, topics):
        self._request('subscribe', topics=topics)

    def consume(self, num_messages, timeout):
        return [JavaKafkaRecord(record) for record in self._request('consume',
            limit=num_messages, timeout_ms=max(1, int(timeout * 1000)))['records']]

    def commit(self, offsets, asynchronous=False):
        if asynchronous:
            raise ValueError('Java consumer commits must be synchronous')
        self._request('commit', offsets=[vars(partition) for partition in offsets])
        return []

    def close(self):
        try:
            if self.process.poll() is None:
                self._request('close')
        finally:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process.stdout.close()

