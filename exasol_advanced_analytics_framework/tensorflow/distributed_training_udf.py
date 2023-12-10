import structlog
import tensorflow as tf
from structlog.typing import FilteringBoundLogger
from tensorflow.python.distribute.cluster_resolver import SimpleClusterResolver, ClusterResolver
from tensorflow.python.keras import activations
from tensorflow.python.training.server_lib import ClusterSpec

from exasol_advanced_analytics_framework.udf_communication.distributed_udf import DistributedUDF, \
    UDFCommunicatorFactory
from exasol_advanced_analytics_framework.udf_communication.exchange_cluster_information import \
    exchange_cluster_information, reserve_port, WorkerAddress, ClusterInformation

LOGGER: FilteringBoundLogger = structlog.get_logger()


class DistributedTrainingUDF(DistributedUDF):

    def run(self, ctx, exa, udf_communicator_factory: UDFCommunicatorFactory):
        with udf_communicator_factory.create() as communicator:
            ip = communicator.listen_ip
            with reserve_port(ip) as port:
                worker_address = WorkerAddress(ip_address=ip, port=port)
                cluster_information = exchange_cluster_information(communicator, worker_address)
                LOGGER.info("after exchange_cluster_information", worker_address=worker_address)
                tf_cluster_resolver = self._create_tf_cluster_resolver(cluster_information, worker_address)
                instance = ctx.i
        # We can only create the tensorflow cluster after we closed the communicator,
        # because otherwise the communicator might loose messages, it is not clear why this happens,
        # but it seeems tensorflow interferes somehow. Other, loads like computations or sleeping doesn't interfere.
        # A potential reason for the interfences could be that tensorflow waits inside of a native function
        # for the connection, which would block the BackgroundThread from working.
        self._train_distributed(tf_cluster_resolver, ctx, instance)

    def _train_distributed(self, cluster_resolver: ClusterResolver, ctx, instance):
        strategy = self._create_distribution_strategy(cluster_resolver)
        with strategy.scope():
            self._train(ctx, instance)

    def _train(self, ctx, instance):
        train_dataset = self._get_train_dataset(ctx)
        model = self._build_and_compile_model()
        history = model.fit(train_dataset, epochs=1)
        for loss in history.history["loss"]:
            ctx.emit(instance, loss)

    def _create_distribution_strategy(self, cluster_resolver):
        tf.config.threading.set_inter_op_parallelism_threads(1)
        tf.config.threading.set_intra_op_parallelism_threads(1)
        communication_options = tf.distribute.experimental.CommunicationOptions(
            implementation=tf.distribute.experimental.CommunicationImplementation.RING)
        strategy = tf.distribute.MultiWorkerMirroredStrategy(
            communication_options=communication_options,
            cluster_resolver=cluster_resolver
        )
        return strategy

    def _create_tf_cluster_resolver(self,
                                    cluster_information: ClusterInformation,
                                    worker_address: WorkerAddress) -> ClusterResolver:
        tf_cluster_spec = ClusterSpec({
            'worker': [f"{worker.ip_address.ip_address}:{worker.port.port}"
                       for worker in cluster_information.workers]

        })
        LOGGER.info("cluster_spec", tf_cluster_spec=tf_cluster_spec)
        tf_cluster_resolver = SimpleClusterResolver(
            cluster_spec=tf_cluster_spec,
            task_type="worker",
            task_id=cluster_information.workers.index(worker_address)
        )
        LOGGER.info("tf_cluster_resolver created")
        return tf_cluster_resolver

    def _generator(self, ctx):
        ctx.reset()
        while True:
            x = ctx.x
            y = ctx.y
            yield x, y
            if not ctx.next():
                break

    def _get_dataset(self, ctx):
        ot = (tf.float32, tf.float32)
        os = (tf.TensorShape([]), tf.TensorShape([]))
        ds = tf.data.Dataset.from_generator(lambda: self._generator(ctx),
                                            output_types=ot,
                                            output_shapes=os)
        options = tf.data.Options()
        options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.OFF
        ds = ds.with_options(options)
        return ds

    def _get_train_dataset(self, ctx):
        ds = self._get_dataset(ctx)
        ds = ds.shuffle(100)
        ds = ds.batch(10)
        return ds

    def _build_and_compile_model(self):
        model = tf.keras.Sequential([
            tf.keras.layers.InputLayer(input_shape=(1)),
            tf.keras.layers.Dense(10, activation=activations.relu),
            tf.keras.layers.Dense(1, activation=activations.linear)
        ])
        model.compile(
            loss=tf.keras.losses.mean_squared_error,
            optimizer=tf.keras.optimizers.SGD(learning_rate=0.01),
            metrics=tf.keras.metrics.mean_squared_error)
        return model
