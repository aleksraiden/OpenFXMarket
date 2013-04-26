/**  openFXMarket - API Engine **/

var sys 	= require('sys'),
    net		= require('net'),
	http	= require('http'),
	url     = require('url'),
	query   = require('querystring'),
	events  = require("events"),
	eye 	= require('./lib/eyes'),
	crypto 	= require('crypto'),
	redis   = require("./lib/node_redis2/index"),
	async   = require('./lib/async'),	
	Buffer 	= require('buffer').Buffer;
	
var _ = require('./lib/underscore');
var emitter = new events.EventEmitter();
var __HOST__ = 'openfxmarket.com'; //test
var __STARTUP__ = new Date().getTime(); // время запуска сервера 

//исключение, которое нигде не было перехвачено ранее
process.addListener('uncaughtException', function (err){
  sys.puts('\n\n');
  sys.log('Caught exception: ' + err.message);
  sys.log(eye.inspect(err.stack));
  sys.puts('\n\n');
  
  return true;  
});

sys.puts('\n\n');
sys.puts(' ====== openFxMarket.com ======');
sys.puts(' ====== 2013 (c) AGPsource.com ====== ');
sys.puts(' ====== Info: aleks.raiden@gmail.com ====== \n\n\n');
sys.puts(' ====== API Engine 0.1 at ' + __HOST__ + ' (Dev version) ======= ');
sys.puts('\n\n');

// общий обьект настроек сервера
var options = {
	host : 'api.' + __HOST__,
	
	httpPort: 9090, //на каком порту у нас HTTP-API
	socketPort: 9091, //прямой порт для сокет-апи 
	websocketPort: 8443, //порт для вебсокет апи 
	
	//RedisDB server 
	redisPort: 6380,
	redisHost: 'localhost',
	redisConfig: {
		parser: "javascript", //использовать hiredis, если есть возможность собрать и подключить 
		//Setting this option to false can result in additional throughput at the cost of more latency.
		socket_nodelay: true, 
		enable_offline_queue: true
	},
		
	//канал в редисе Pub/Sub для поступающих котировок 
	redisOrdersStream: 'MQE_ORDERS_CHANNEL', 
	//управляющий канал (команды, отмены и т.п.)
	redisControlsStream: 'MQE_CONTROLS_CHANNEL',
	//где лежат данные с авторизациями юзеров 
	redisUserAccountsAuth: 'MQE_AUTH_ACCOUNTS',
	//канал для оповещения других нод о своей работе 
	redisNodeNotificator: 'MQE_NODES_NOTIFY_CHANNEL',
	//канал, куда паблишим ордера, которые удачно добавлены в очереди
	redisAcceptedOrdersStream: 'MQE_ACCEPTED_ORDERS_CHANNEL',
	//стрим для публикации последней котировки 
	redisLastQuoteStream: 'MQE_LAST_QUOTES_CHANNEL',
	
	
	
	
	
	
	
	
	
	//это список ордеров, которые сматчили, но они по какой-то причине не дошли до расчета по аккаунтам  
	redisMatchedOrdersQueue: 'MQE_MATCHED_ORDERS',
	//префикс для ордербуков (добавляем код инструмента в верхнем регистре)
	redisOrderbookPrefix: 'MQE_ORDERBOOK_',
	//название ключа для топовой котировки (хеш со всеми последними котировками (топ-оф-бук)
	redisCurrentQuoteHash: 'MQE_LAST_QUOTES',
	
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
	
	//хеш для истории всех ордеров 
	redisAllOrdersDB: 'MQE_ORDERS_DB',
	//для хранения експайров надо отдельный сортед сет (чтобы выбирать одним запросом)
	redisOrderExpiresSet: 'MQE_EXPIRES_ORDERSTORE_',
	//глобальная таблица статусов ордеров (hash table)
	redisGlobalOrderStatus: 'MQE_GLOBAL_ORDERS_STATUS',
	
	//канал для трансляции ид снятых ордеров 
	redisCanceledOrdersStream: 'MQE_CANCELED_ORDERS_CHANNEL',
	
	
	//в каком формате принимать котировки (пока json, потом возможно более эффективный типа messagepack или protobuf)
	defaultOrderFormat: 'json',
	
	//есть ли ограничение на глубину стакана 
	maxOrderbookDepth: 1000000,
	
	//есть ли ограничение на время жизни ордера (0 - нет, пока не будет исполнен или снят)
	maxOrderLifetime: 0,
	
	//интервал чека стакана на предмет матчинга (в миллисекундах)
	orderMatchInterval: 1000,
	
	//как часто проверять експайринг (для топ-оф-бук он проверяется при каждом матчинге)
	orderExpaireInterval: 1000,
	
	//сколько может быть паралельных торговых очередей (инструментов)
	maxAssetsBook: 1000,
	
	//отдельное логгирование поступающих заявок и т.п. в отдельный сервер levelDB
	useAdvancedLog: false,
	logPort: null,
	logHost: 'localhost',
	
	//системный таймер, периодическое обслуживание сервера
	sysOpsTimer: 30000,	
	
	//как часто публиковать котировку (Top-of-Book)
	bestQuotePublishInterval: 1000,
	
	//сколько из топа выбирать ордеров для матчинга за один раз? 
	matchAtOnceOrders: 1,
	
	//список известных инструментов системе 
	//эти данные нужны и account-сервису для расчета баланса.
	assets : [],
	
	//в тестовом варианте дефолтный конфиг
	__defaultAssetConfig: {
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
		},
	
	//локальная копия списка с ошибками
	lastErroredOrderIds:[],
	lastMatchedOrderIds: [], //ид ордеров, которые отменены или сматченные, которые можно убирать 
	maxLocalErrored: 1000000 //сколько максимум храним ошибочных ордеров 
	
	
};
	//последняя котировка 
	var __LAST_ORDER__ = {};
	var __BEST_ORDER__ = {};
	
	var apiServer = {
		http: 		null,  //HTTP-API сервер 
		socket: 	null,  //для soсket-сервера
		websocket: 	null,
		redis: 		null,
		
		//массив какие методы нам доступны 
		avalaibleActions: [
			'/status', //Статус сервера и апи 
			'/login',
			'/trade/assets', //инфо про все торговые очереди 
			'/orderbook/top', //топ ордербука - 1
			'/orderbook/10', //топ ордербука - 10
			'/orderbook/25', //топ ордербука - 25
			'/orderbook/50', //топ ордербука - 50
			'/orderbook/100', //топ ордербука - 100
			'/orderbook/full', //полностью ордербук на всю глубину 
			'/order/last', //последняя котировка
			'/order/best',
			'/order/get', //сразу ласт и бест 
			'/order/add', // добавление новой котировки 
			'/order/cancel', //снять котировку
			'/order/status',  //статус котировки

			'account/orders' //все ордера аккаунта
						
			]
	};

	//Глобальный флаг, включает дебаг-режим протокола редиса 
	redis.debug_mode = false;
	
	//это выделенное подключение для Pub/Sub
	var pubsub = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
	pubsub.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	pubsub.on("connect", function (err){
		sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
		
		pubsub.subscribe([options.redisOrdersStream, options.redisControlsStream, options.redisAcceptedOrdersStream, options.redisLastQuoteStream]);
	});
	
	//слушаем каналы 
	pubsub.on("message", function(ch, msg){
		if (_.isEmpty(msg)) return;
	
		try 
		{
			if (msg.indexOf('{"_":') === 0)
			{
				var _msg = JSON.parse(msg);
				
				//сдублируем, чтобы потом не тратиться на перевод в строку 
				_msg.__json = msg;
			}


				if (ch == options.redisOrdersStream)
				{
					//у нас новый ордер 
//sys.log('[NEW ORDER] ' + _msg._ + ' / ' + _msg.a);
//sys.puts(eye.inspect( _msg ));
					emitter.emit('MQE_newOrder', _msg);
				}
				else
				if (ch == options.redisControlsStream)
				{
					//у нас новая команда управления 
					emitter.emit('MQE_controlAction', _msg);
				}
				else
				//сюда транслируют последние и лучшие котировки 
				if (ch == options.redisLastQuoteStream)
				{
					if (_msg.type == 'best')
					{
						__BEST_ORDER__[ _msg.code ] = _msg;					
					}
					else
					if (_msg.type == 'last')
					{
						__LAST_ORDER__[ _msg.code ] = _msg;					
					}
				
				}
			}
		}catch(e){
			sys.puts('\n===============================================\n');
			sys.log('Caught exception: ' + e.message);
			sys.log(eye.inspect(e.stack));
			sys.puts('\n===============================================\n');
		}
	});
	

	//а теперь клиент для ордербуков 
	var datadbs = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
	datadbs.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	datadbs.on("connect", function (err){
		
		emitter.emit('MQE_loadStartUpConfig', function(){
			emitter.emit('MQE_readyToWork');
		});
	
		sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
	});
	
	
	
//=================
//загружает конфигурацию торговых очередей и инструментов, потом вызивает уже стартовый калбек 
emitter.addListener('MQE_loadStartUpConfig', function(callBack){
	//для начала достанем конфигурацию
	datadbs.hgetall(options.redisAssetsTradeConfig, function(err, data){
		sys.puts( eye.inspect( data ) ) ;
		
		if (_.isEmpty(data))
		{
			//клонируем дефолт 
			data = [];
			data.push( _.clone(options.__defaultAssetConfig) );
		}
		
		_.each(data, function(obj){
			sys.log('[INFO] Support asset: ' + obj.code + ' / ' + obj.desc);
				
			options.assets.push( obj );
		});		
		
					
		//оповестим всех, что мы работаем 
		datadbs.publish(options.redisNodeNotificator, JSON.stringify({
			node: 'api',
			startTs: __STARTUP__,
			status: 'startup'
		}));
				
		if (_.isFunction(callBack))
			callBack();
	});	
});

//сигнал, что мы можем начинать работать 
emitter.addListener('MQE_readyToWork', function(){
	
	//создаем HTTP-сервер 
	apiServer.http = http.createServer(function(req, resp){
		//принимаем только GET/POST методы 
		if (req.method == 'GET') || (req.method == 'POST')
			emitter.emit('MQE_HTTP_REQUEST', req, resp);
		else
		{
			resp.writeHead(403, 'Unsupported HTTP Method');
			resp.end();		
		}
	});
	
	//скорее служебный листенер, реально ни для чего не надо ) 
	apiServer.http.addListener('connection', function(socket){
		sys.log('[HTTP] New connection established');
	});
	
	//для последующего повышения до вебсокета 
	apiServer.http.addListener('upgrade', function(req, socket, head){
		
	});
	
	//биндим для прослушки порта 
	apiServer.http.listen(options.httpPort, 'localhost', 511, function(){
		sys.log('[HTTP] OK! HTTP server started and bound to listen their port.');
	});	

	
	sys.log('  ====  All subsystem ON! Ready to Work! ==== ');
});

//основной обработчик запросов по HTTP 
emitter.addListener('MQE_HTTP_REQUEST', function(req, resp){
	//sys.puts(  sys.inspect(req) );
	var _url = url.parse(req.url, true);
	
	if (_.indexOf(apiServer.avalaibleActions, _url.pathname) == -1)
	{
		resp.writeHead(404, 'Unsupported command');
		resp.end();	
		
		return;
	}
	
	
	//теперь обработка команд 
	if (_url.pathname == '/order/add')
	{
		//добавление одной или нескольких котировок (передаются как JSON)
		//нам надо распарсить, проверить их, привести к формату, потом сгенерировать ид, отправить их на биржу,
		//потом дождаться акцепта и вернуть ид-шники 
		
		
	
	}
	
	
	
});







//генерируем индикативную котировку по Top-of-Book
emitter.addListener('MQE_generateBestQuote', function(a){
	
	async.parallel([
			function(callBack){
				//для котировки buy выберем топ от sell-очереди 
				ordbs.zrangebyscore([
					options.redisOrderbookPrefix + a.toUpperCase() + '_S',
					'(0', //минимальная цена +1 поинт - используем фичу синтаксиса редиса 
					'(999999999',
					'LIMIT',
					0,
					1], 
				function(err, data){
					if ((!err) && (data.length > 0))
					{
						//поскольку мы можем выбирать несколько котировок, обойдемся одним вызовом JSON.parse 
						var _data = JSON.parse('[' + data.join(',') + ']');
						
						_.each(_data, function(x, i){
							_data[i]['__json'] = data[i];
						});
						
						//это котировка с минимальной ценой продажи 
						callBack(null, _data[0] );
					}
					else
						callBack(1, null );
				});
			},
			function(callBack){
				//для котировки sell выберем топ от buy-очереди 
				ordbs.zrevrangebyscore([
					options.redisOrderbookPrefix + a.toUpperCase() + '_B',
					'(999999999', //минимальная цена +1 поинт - используем фичу синтаксиса редиса 
					'(0',
					'LIMIT',
					0,
					1], 
				function(err, data){
					if ((!err) && (data.length > 0))
					{
						//поскольку мы можем выбирать несколько котировок, обойдемся одним вызовом JSON.parse 
						var _data = JSON.parse('[' + data.join(',') + ']');
						
						_.each(_data, function(x, i){
							_data[i]['__json'] = data[i];
						});
						
						//это котировка с Максимальной ценой покупки 
						callBack(null, _data[0] );
					}
					else
						callBack(1, null );
				});		
			}],
			function(err, result){
				if (!err)
				{
					//если же мы не нашли одной из котировок - она может или отбрасываться или ждать своего часа
					//var _best_sell = result[0], _best_buy = result[1];
								
					var bestQuote = {
						type: 'best',
						code: a,
						ask: result[0].p,
						bid: result[1].p,
						timestamp: result[0].d
					};
					
					if (result[1].d > result[0].d)
						bestQuote.timestamp = result[1].d;
					
					var _json = JSON.stringify( bestQuote );
					//теперь публикуем в пабсаб и ложим в хеш последних котировок
					ordbs.hset(options.redisCurrentQuoteHash, a, _json);
					
					ordbs.publish(options.redisLastQuoteStream, _json);	

					sys.log('[BEST] ' + bestQuote.ask + ' / ' + bestQuote.bid );
					
					//а ласт у нас и так есть 
					if (_.isUndefined( __LAST_ORDER__[ a.toUpperCase() ] ))
						return;	
					
					var _last = __LAST_ORDER__[ a.toUpperCase() ];
					
					var lastQuote = {
						type: 'last',
						code: a,
						ask: null,
						bid: null,
						timestamp: 0
					};
					
					if (!_.isUndefined(_last['S']))
						lastQuote.ask = _last['S'].p;					
					
					if (!_.isUndefined(_last['B']))
						lastQuote.bid = _last['B'].p;
						
					if (_last['S'].d >= _last['B'].d)
						lastQuote.timestamp = _last['S'].d;
					else
						lastQuote.timestamp = _last['B'].d;
						
					if (lastQuote.timestamp == 0)
						lastQuote.timestamp = new Date().getTime();
						
					//паблишим котировку 
					ordbs.publish(options.redisLastQuoteStream, JSON.stringify(lastQuote));
					sys.log('[LAST] ' + lastQuote.ask + ' / ' + lastQuote.bid );					
				}
	});

});


//метод проходит по всем котировкам и показывает лучшие
emitter.addListener('MQE_publishAllBestQuote', function(){
	_.each(options.assets, function(x){
		if (x.trade == 'open')
		{
			emitter.emit('MQE_generateBestQuote', x.code);
		}	
	});
});







//===========================================================================================================
//sys.log(' ===== Setting up runtime ======= ');
//===========================================================================================================
