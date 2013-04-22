/**  metaExchange AccountEngine **/

var sys 	= require('sys'),
    net		= require('net'),
	events  = require("events"),
	eye 	= require('./lib/eyes'),
	crypto 	= require('crypto'),
	RJSON   = require('./lib/rjson'),
	redis   = require("./lib/node_redis2/index"),
	async   = require('./lib/async'),	
	Buffer 	= require('buffer').Buffer;
	
var _ = require('./lib/underscore');



var emitter = new events.EventEmitter();
var __HOST__ = 'openfxmarket.com'; //test
var __STARTUP__ = new Date().getTime(); // время запуска сервера 

//сведенья о последней ошибке 
var lastUncaughtException = {
	error: '',
	errorAt: 0
};

emitter.addListener('error', function(exc){
	sys.log(eye.inspect(exc));
});

sys.puts('\n\n');
sys.puts(' ====== openFxMarket.com ====== \n');
sys.puts(' ====== 2013 (c) AGPsource.com ====== \n');
sys.puts(' ====== metaQuote Account Engine 0.1 at ' + __HOST__ + ' (Test) ======= ');
sys.puts('\n\n');

// общий обьект настроек сервера
var options = {
	//конфиг для Flash socket server 
	encoding : 'utf8',
	port: 443,
	host : 'exchange.' + __HOST__,
	
	//RedisDB server 
	redisPort: 6380,
	redisHost: 'localhost',
	redisConfig: {
		parser: "javascript", //использовать hiredis, если есть возможность собрать и подключить 
		//Setting this option to false can result in additional throughput at the cost of more latency.
		socket_nodelay: true, 
		enable_offline_queue: true
	},
		
	//управляющий канал (команды, отмены и т.п.)
	redisControlsStream: 'MQE_ACCOUNT_CONTROLS_CHANNEL',
	//это список ордеров, которые сматчили, но они по какой-то причине не дошли до расчета по аккаунтам  
	redisMatchedOrdersQueue: 'MQE_MATCHED_ORDERS',
	//канал, куда паблишим ордера, которые удачно добавлены в очереди
	redisAcceptedOrdersStream: 'MQE_ACCEPTED_ORDERS_CHANNEL',
	//канал, куда постим оредра, которые сматчены между собой (команды для изменения аккаунта)
	redisMatchedOrdersStream: 'MQE_MATCHED_ORDERS_CHANNEL',
	//это ордера просроченные 
	redisExpiredOrdersStream: 'MQE_EXPIRED_ORDERS_CHANNEL',
	//для случая, когда отваливается слушатель списка подтвержденных ордеров 
	redisAcceptedOrdersQueue: 'MQE_ACCEPTED_ORDERS',
	//канал, куда сообщаем ид ордеров, которые не прошли 
	redisErroredOrdersStream: 'MQE_ERRORED_ORDERS_CHANNEL',
	//где храним, по каким инструментам торгуем 
	redisAssetsTradeConfig: 'MQE_ASSETS_CONFIG',
	//глобальная таблица статусов ордеров (hash table)
	redisGlobalOrderStatus: 'MQE_GLOBAL_ORDERS_STATUS',
	//список для всех ордеров с ошибками 
	redisErroredOrders: 'MQE_ERRORED_ORDERS',
	
	//аккаунты - для системных акаунтов используем стандартные INT редиса
	redisSYSTEM_ACCOUNT: 'MQE_ACCOUNT_SYSTEM', //системный акаунт 
	redisSYSTEM_ACCOUNT_TAX: 'MQE_ACCOUNT_SYSTEM_TAX', //аккаунт для сброса сюда комиссии 
	redisUSERS_ACCOUNT: 'MQE_ACCOUNT_USERS', //пользовательские аккаунты (в виде hset-a)
	
	//храним две структуры - <account_id> => list order id's  и общий хеш = order_id => account_id 
	redisOrdersPerAccount: 'MQE_ORDERS_ACC_', //добавим id аккаунта 
	redisOrderToAccount: 'MQE_ORDER_TO_ACCOUNT',
	
	
	//в каком формате принимать котировки (пока json, потом возможно более эффективный типа messagepack или protobuf)
	defaultOrderFormat: 'json',
	
	//отдельное логгирование поступающих заявок и т.п. в отдельный сервер levelDB
	useAdvancedLog: false,
	logPort: null,
	logHost: 'localhost',
	
	//системный таймер, периодическое обслуживание сервера
	sysOpsTimer: 30000,	
	
	//список известных инструментов системе 
	//эти данные нужны и account-сервису для расчета баланса.
	assets : {
		'BTC/USD': {
			code: 'BTC/USD',
			desc: 'Валютная пара Bitcoin/USD',
			trade: 'open',
			tradeTiming:[0, 24*3600], //диапазон в секундах, от начала суток, в котором торгуется инструмент 
			
			avalaibleOrderTypes:['L','M'], //какие типы ордеров разрешены (L - limit, M - market)
			price_code: 'USD', //код инструмента, в котором выражена цена 
			asset_code: 'BTC', // код инструмента, который торгуется (на который заключены контракты)
			
			price_min: 0.001, //минимальная цена и минимальный шаг цен 
			asset_min: 0.001, //минимальный размер лота
			
			//это максимальные цены и обьемы 
			price_max: 99999,
			asset_max: 99999,			
			
			//коммисия, которая взымается с операции (всегда берется от цены инструмента)
			tax: {
				sell: 0.001, //коммисия с продажи 
				buy:  0.001  // коммисия с покупки
			}			
		}	
	},
	
	//локальная копия списка с ошибками
	lastErroredOrderIds:[],
	lastMatchedOrderIds: [], //ид ордеров, которые отменены или сматченные, которые можно убирать 
	maxLocalErrored: 1000000 //сколько максимум храним ошибочных ордеров 
	
	
};

//исключение, которое нигде не было перехвачено ранее
process.addListener('uncaughtException', function (err){
  sys.puts('\n\n');
  sys.log('Caught exception: ' + err.message);
  sys.log(eye.inspect(err.stack));
  sys.puts('\n\n');
  
  lastUncaughtException.error = err.message;
  lastUncaughtException.errorAt = new Date().getTime();
  
  return true;  
});

	redis.debug_mode = false;
	
	//это выделенное подключение для Pub/Sub
	var pubsub = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
	pubsub.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	pubsub.on("connect", function (err){
		sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
		
		pubsub.subscribe([options.redisAcceptedOrdersStream, options.redisControlsStream, options.redisErroredOrdersStream, options.redisMatchedOrdersStream, options.redisExpiredOrdersStream]);
	});
	
	//слушаем каналы 
	pubsub.on("message", function(ch, msg){
		if ((_.isEmpty(msg)) || (_.isEmpty(ch))) return;
//sys.puts('\n---\n' + ch + '\n---\n' + eye.inspect(msg) + '\n');		
	
		try 
		{
			if ( (msg.indexOf('{"_":') === 0))
			{
				msg = JSON.parse(msg);
			}
			
				
			//это подтверждение, что ордер поставлен в торговую очередь 
			if (ch == options.redisAcceptedOrdersStream)
			{
				//у нас новый ордер 
//sys.log('[NEW ORDER] ' + _msg._ + ' / ' + _msg.a);
				emitter.emit('MQE_acceptOrderId', msg);
			}
			else
			if (ch == options.redisErroredOrdersStream)
			{
				//мы получили ордер с ошибкой (__error)
				emitter.emit('MQE_errorOrder', msg);
			}
			else
			if (ch == options.redisExpiredOrdersStream)
			{
				//ордера, которые истекли по времени 
				emitter.emit('MQE_expiredOrder', msg);
			}
			else
			if (ch == options.redisMatchedOrdersStream)
			{
				//сматченные ордера
				emitter.emit('MQE_processAccountByOrder', msg);
			}
		}catch(e){
			sys.puts('\n===============================================\n');
			sys.puts('Caught exception: ' + e.message);
			sys.puts(eye.inspect(e.stack));
			sys.puts('\n===============================================\n');
		}
	});
	

	//а теперь клиент для основных операций 
	var acdbs = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
		acdbs.on("error", function (err){
			sys.log("[ERROR] Redis error " + err);
		});
		
		acdbs.on("connect", function (err){
			acdbs.select(2);
			sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
		});
	
//=================

//сохраняем подтверждение по ордеру (по ид)
emitter.addListener('MQE_acceptOrderId', function(orderId){
	//sys.log( ' =================> ACCEPT: ' + orderId );
	if (!_.isEmpty(orderId))
	{
		//в глобальном хешмапе обновить или установить статус ордера 
		acdbs.hset(options.redisGlobalOrderStatus, orderId, '[OK] addMarketQueue', function(err, data){
			if (!err)
			{
				sys.log('[OK] Order #' + orderId + ' status: addMarketQueue');
			}
			else
				sys.log('[ERROR] Error while save/update order status #' + orderId);
		});	
	}
});

//обработчик ошибок 
emitter.addListener('MQE_errorOrder', function(order){
	if (typeof(order._) != 'undefined')
	{
		//запишем в глобальный статус 
		acdbs.hset(options.redisGlobalOrderStatus, order._, '[ERROR] ' + order.__error);
		
		//запишем себе ошибочный еррор 
		acdbs.hset(options.redisErroredOrders, order._, JSON.stringify(order));
		
		// !TODO: наверное оповещать еще какое-то внешнее апи? 	
	}
});

//ордера которые истекли и автоматически сняты
emitter.addListener('MQE_expiredOrder', function(orderId){
	if (typeof(orderId) != 'undefined')
	{
		//сначала установим статус в ошибку
		acdbs.hset(options.redisGlobalOrderStatus, orderId, '[EXPIRED] Order expired');
		
		sys.log('[EXPIRED] Order #' + 	orderId + ' has marked as EXPIRED');	
		// !TODO: наверное оповещать еще какое-то внешнее апи? 	
	}
});

//Основной обработчик - он корректирует баланс юзера 
emitter.addListener('MQE_processAccountByOrder', function(order){
	//sys.puts('\n========= Process Buy/Sell matched order ============ \n');
	//sys.puts( eye.inspect( order ) );
	
	//для начала - получим ид аккаунта, связанного с этим ордером. 
	acdbs.hget(options.redisOrderToAccount, order._, function(err, accountId){
		if (!err)
		{
			if (_.isEmpty(accountId))
			{
				//аккаунта не создано
				//в тесте - мы просто создадим 
				accountId = _.random(100, 1000); //в тесте достаточно аккаунтов
				sys.log('[WARNING] Account ID not assigned, generate new: ' + accountId);
				
				//запишем аккаунт 
				acdbs.hset(options.redisOrderToAccount, order._, accountId);			
			}
			
			//теперь получим аккаунт юзера 
			acdbs.hget(options.redisUSERS_ACCOUNT, accountId, function(err, acc){
				if (!err)
				{
					if (!_.isEmpty(acc))
					{
						acc = JSON.parse(acc);				
					}
					else
					{
						acc = {
							'accountId' : accountId,
							'createdAt' : new Date().getTime(),
							'lastActivity' : new Date().getTime(),
							
							'matchedOrders': 0, //счетчик сделок 
							
							'balance' : {
								'USD' : 0.000000,
								'BTC' : 0.000000
							}
						};
						
						acdbs.hset(options.redisUSERS_ACCOUNT, accountId, JSON.stringify(acc));				
					}
					
					//теперь рассчитаем все выплаты
					
					//получим настройки торговой очереди 
					var tradeOpt = options.assets[ order.a.toUpperCase() ];
					
					//комисия пока фиксирована 
					if (order.t == 'b')		//покупка 
						var tax = tradeOpt.tax.buy;
					else
					if (order.t == 's') //продажа 
						var tax = tradeOpt.tax.sell;
						
					//теперь от суммы отнимем комисию 
					var size = (order.matched.matchV * order.matched.matchP) - tax;
					
					//теперь скорректируем новый баланс юзера 
					
					if (order.t == 'b')	
					{
						//покупка - отдаем валюту price, получаем актив 
						acc.balance[ tradeOpt.price_code ] = Number(acc.balance[ tradeOpt.price_code ] - size).toFixed(6);
						
						acc.balance[ tradeOpt.asset_code ] = Number(acc.balance[ tradeOpt.asset_code ] + order.matched.matchV).toFixed(6);
					}
					else
					if (order.t == 's')	
					{
						//продажа - отдаем актив, получаем валюту 
						acc.balance[ tradeOpt.price_code ] = Number(acc.balance[ tradeOpt.price_code ] + size).toFixed(6);
						
						acc.balance[ tradeOpt.asset_code ] = Number(acc.balance[ tradeOpt.asset_code ] - order.matched.matchV).toFixed(6);
					}
					
					acc.matchedOrders++;
					
					//теперь выполняем все 
					async.parallel([
						function(callBack){
							//изменим системный аккаунт также 
							acdbs.incrby( options.redisSYSTEM_ACCOUNT_TAX, Math.ceil( tax * 1000 ), function(err, data){
								if (!err)
									callBack(null);
								else
									callBack(1, data);
							});
						},
						function(callBack){
							//изменяем аккаунт 
							acdbs.hset(options.redisUSERS_ACCOUNT, accountId, JSON.stringify(acc), function(err, data){
								if (!err)
									callBack(null);
								else
									callBack(1, data);
							});
						},
						function(callBack){
							//обновим статус ордера 
							acdbs.hset(options.redisGlobalOrderStatus, order._, '[MATCH]' + JSON.stringify(order.matched), function(err, data){
								if (!err)
									callBack(null);
								else
									callBack(1, data);
							});
						}],
						function(err, results){
							if (!err)
							{
								sys.log('[MATCH] ('+accountId+') Order '+order.t+'#' + order._ + ' processed by: ' + order.matched.matchV + ' vol. at price ' + order.matched.matchP + '. Tax: ' + tax);							
							}						
						}
					);				
				}			
			});


				
		}
		else
			sys.log('[ERROR] Error while obtain accountId');
	});
	
	
});











//===========================================================================================================
sys.log('\n ===== Setting up runtime ======= \n');
//===========================================================================================================



/**
setInterval(function(){
	
	_.each(options.assets, function(x){
		if (x.trade == 'open')
		{
			emitter.emit('MQE_selectTopBook', x.code);
		}	
	});

}, options.orderMatchInterval);
**/


