const connectionHelper = require('../../reverse_engineering/helpers/connectionHelper');
const loggerHelper = require('./logHelper');

const applyToInstanceHelper = {
    async applyToInstance(data, logger, cb) {
		const log = loggerHelper.createLogger({
			title: 'Applying to instance',
			hiddenKeys: data.hiddenKeys,
			logger,
		});

		try {
			logger.clear();
			log.info(loggerHelper.getSystemInfo(data.appVersion));
			log.info(data);

			const { _client } = connectionHelper.connect(connectionInfo);

			connectionHelper.disconnect();
			cb(null);
		} catch (error) {
			log.error(error);
			connectionHelper.disconnect();

			cb({
				message: error.message,
				stack: error.stack,
			});
		}
    },

    testConnection(connectionInfo, logger, cb){
		const log = loggerHelper.createLogger({
			title: 'Test connection',
			hiddenKeys: connectionInfo.hiddenKeys,
			logger,
		});

        try {
			logger.clear();
			log.info(loggerHelper.getSystemInfo(connectionInfo.appVersion));
			log.info(connectionInfo);

			const { _client } = connectionHelper.connect(connectionInfo);

			_client.ping({ requestTimeout: 5000 }, (error, success) => {
				connectionHelper.disconnect();
				if (error) {
					log.error(error);
				}

				log.info('Connected successfully');
				cb(!success);
			});
		} catch (error) {
			log.error(error);
			
			return cb({
				message: error.message,
				stack: error.stack,
			});
		}

    }
};

module.exports = applyToInstanceHelper;