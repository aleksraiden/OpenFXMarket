/**  metaExchange AccountEngine **/

var sys 	= require('sys'),
    net		= require('net'),
	events  = require("events"),
	eye 	= require('./lib/eyes'),
	crypto 	= require('crypto'),
	RJSON   = require('./lib/rjson'),
	redis   = require("./lib/node_redis3/index"),
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
	//для случая, когда отваливается слушатель списка подтвержденных ордеров 
	redisAcceptedOrdersQueue: 'MQE_ACCEPTED_ORDERS',
	//канал, куда сообщаем ид ордеров, которые не прошли 
	redisErroredOrdersStream: 'MQE_ERRORED_ORDERS_CHANNEL',
	//где храним, по каким инструментам торгуем 
	redisAssetsTradeConfig: 'MQE_ASSETS_CONFIG',
	//глобальная таблица статусов ордеров (hash table)
	redisGlobalOrderStatus: 'MQE_GLOBAL_ORDERS_STATUS',
	
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
	assets : [
		{
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
	],
	
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
		
		pubsub.subscribe([options.redisAcceptedOrdersStream, options.redisControlsStream, options.redisErroredOrdersStream]);
	});
	
	//слушаем каналы 
	pubsub.on("message", function(ch, msg){
		if (_.isEmpty(msg)) return;
	
		try 
		{
			if (((ch == options.redisAcceptedOrdersStream) || (ch == options.redisErroredOrdersStream) || (ch == options.redisControlsStream)) && (msg.indexOf('{"_":') === 0))
			{
				var _msg = JSON.parse(msg);
				
				//сдублируем, чтобы потом не тратиться на перевод в строку 
				_msg.__json = msg;
				
				//TODO: дополнительно проверять формат сообщений 
				if (typeof(_msg) != 'object') return;
/**
				if (ch == options.redisOrdersStream)
				{
					//у нас новый ордер 
//sys.log('[NEW ORDER] ' + _msg._ + ' / ' + _msg.a);
					emitter.emit('MQE_newOrder', _msg);
				}
				else
				if (ch == options.redisControlsStream)
				{
					//у нас новая команда 
					emitter.emit('MQE_newOrder', _msg);
				}
**/				
			}
		}catch(e){
			sys.puts('\n===============================================\n');
			sys.puts('Caught exception: ' + e.message);
			sys.puts(eye.inspect(e.stack));
			sys.puts('\n===============================================\n');
		}
	});
	

	//а теперь клиент для ордербуков 
	var acdbs = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
		acdbs.on("error", function (err){
			sys.log("[ERROR] Redis error " + err);
		});
		
		acdbs.on("connect", function (err){
			sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
		});
	
//=================













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


