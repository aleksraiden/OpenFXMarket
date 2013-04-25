/**  openFXMarket - exchange Engine **/

var sys 	= require('sys'),
    net		= require('net'),
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
sys.puts(' ====== exchange Engine 0.1 at ' + __HOST__ + ' (Dev version) ======= ');
sys.puts('\n\n');

// общий обьект настроек сервера
var options = {
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
		
	//канал в редисе Pub/Sub для поступающих котировок 
	redisOrdersStream: 'MQE_ORDERS_CHANNEL', 
	//управляющий канал (команды, отмены и т.п.)
	redisControlsStream: 'MQE_CONTROLS_CHANNEL',
	//это список ордеров, которые сматчили, но они по какой-то причине не дошли до расчета по аккаунтам  
	redisMatchedOrdersQueue: 'MQE_MATCHED_ORDERS',
	//префикс для ордербуков (добавляем код инструмента в верхнем регистре)
	redisOrderbookPrefix: 'MQE_ORDERBOOK_',
	//название ключа для топовой котировки (хеш со всеми последними котировками (топ-оф-бук)
	redisCurrentQuoteHash: 'MQE_LAST_QUOTES',
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
	//сет для ордеров, которые выбрали для матча, но еще не обработали 
	//redisProcessingOrders: 'MQE_PROCESSED_ORDERS',
	//где храним, по каким инструментам торгуем 
	redisAssetsTradeConfig: 'MQE_ASSETS_CONFIG',
	//стрим для публикации последней котировки 
	redisLastQuoteStream: 'MQE_LAST_QUOTES_CHANNEL',
	//хеш для истории всех ордеров 
	redisAllOrdersDB: 'MQE_ORDERS_DB',
	//для хранения експайров надо отдельный сортед сет (чтобы выбирать одним запросом)
	redisOrderExpiresSet: 'MQE_EXPIRES_ORDERSTORE',
	
	
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
	//последняя котировка 
	var __LAST_ORDER__ = {};

	//Глобальный флаг, включает дебаг-режим протокола редиса 
	redis.debug_mode = false;
	
	//это выделенное подключение для Pub/Sub
	var pubsub = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
	pubsub.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	pubsub.on("connect", function (err){
		sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
		
		pubsub.subscribe([options.redisOrdersStream, options.redisControlsStream]);
	});
	
	//слушаем каналы 
	pubsub.on("message", function(ch, msg){
		if (_.isEmpty(msg)) return;
	
		try 
		{
			if (((ch == options.redisOrdersStream) || (ch == options.redisControlsStream)) && (msg.indexOf('{"_":') === 0))
			{
				var _msg = JSON.parse(msg);
				
				//сдублируем, чтобы потом не тратиться на перевод в строку 
				_msg.__json = msg;
				
				//TODO: дополнительно проверять формат сообщений 
				if (typeof(_msg) != 'object') return;

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
			}
		}catch(e){
			sys.puts('\n===============================================\n');
			sys.log('Caught exception: ' + e.message);
			sys.log(eye.inspect(e.stack));
			sys.puts('\n===============================================\n');
		}
	});
	

	//а теперь клиент для ордербуков 
	var ordbs = redis.createClient(options.redisPort, options.redisHost, options.redisConfig);
	
	ordbs.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	ordbs.on("connect", function (err){
		
		//тестовый код для моего сервера 
		if (options.redisHost == 'scylla.trdata.com')
			ordbs.select(2);
	
		sys.log('[OK] Connected to Redis-server at ' + options.redisHost);
	});
	
//=================

//Поступил новый ордер нам 
emitter.addListener('MQE_newOrder', function(data){
	//по сути, нам ничего особо не нужно - добавить только в ордербук котировку
	//пока временно здесь, потом перенести во фронтенд
	ordbs.hset(options.redisAllOrdersDB, data._, data.__json, function(err, result){
		if (!err)
		{		
			// !TODO: может тоже через async делать? надо подумать про архитектуру  
			ordbs.zadd([ 
					//вида: MQE_ORDERBOOK_BTC/USD_S (sell) или MQE_ORDERBOOK_BTC/USD_B (buy)
					options.redisOrderbookPrefix + data.a.toUpperCase() + '_' + data.t.toUpperCase(),
					data.p,
					data.__json				
					], 
					function(err, response){
						if (!err)
						{
							//добавили? вышлем подтверждение, что мы приняли ордер  
							ordbs.publish(options.redisAcceptedOrdersStream, data._, function(err, resp){
								//но, если там никто не слушает? 
								if ((err) && (resp < 1))
								{
									ordbs.rpush(options.redisAcceptedOrdersQueue, data._);
								}
								
								//запомним последнюю котировку (не применимо к маркет-ордерам) 
								if (data.f[0] == 'L')
								{
									if (_.isUndefined(__LAST_ORDER__[ data.a.toUpperCase() ]))
										__LAST_ORDER__[ data.a.toUpperCase() ] = {S:null,B:null};
										
									__LAST_ORDER__[ data.a.toUpperCase() ][ data.t.toUpperCase() ] = data;
								}

								//для заявок по времени добавить их в сет.
								if (data.с != 0)
									ordbs.zadd([options.redisOrderExpiresSet + data.a.toUpperCase(), data.с, data._]);								
							});
						}
						else
						{
							options.lastErroredOrderIds.push( data._ );
							//запишем текст ошибки 
							data.__error = response;
							
							//оповестим других про ошибочный ордер 
							ordbs.publish(options.redisErroredOrdersStream, data);			
						}
					}
				);
		}
	});
});

//собственно, сам матчинг енжайн 
emitter.addListener('MQE_matchEngine', function(a, sell, buy){
	//sys.log('==== [MATCH ENGINE] ===== ');
	//sys.puts( eye.inspect( [sell, buy] ) );
	//sys.puts('\n======= WARNING! TEST MODE! ============= \n');
	
	//если хотя купить за цену == или больше, чем лучшая заявка на продажу - матчим оба 
	//если хотят продать за цену == или меньше, чем лучшая заявка на покупку - матчим оба 
	
	//для поддержки ордеров с експайром 
	var _nowDt = new Date().getTime();
		
	if ((sell.c != 0) || (buy.c != 0))
	{
		//проверка, может ордера устарели 
		async.parallel([
			//убираем с очереди 
			function(callBack){
				if ((sell.c != 0) && (sell.c <= _nowDt))
				{
					//убрать этот ордер вообще 
					ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_S', sell.__json, function(err, result){
						if (err)
							callBack(1, '[DELETE/Sell(timeout)] ERROR: ' + err + '   ' + result);
						else
						{
sys.log('[EXPIRE] Order ' + sell.t + '  '+sell.f[0]+'#' + sell._ + ' has expired');
							ordbs.publish(options.redisExpiredOrdersStream, sell._, function(err, result){
								if (err)
									callBack(1, '[DELETE/Sell(Notify timeout)] ERROR: ' + err + '   ' + result);
								else
									callBack(null);	
							});	
						}
					});
				}
				else
					callBack(null);
			},
			//так же с бай-ордерами 
			function(callBack){
				if ((buy.c != 0) && (buy.c <= _nowDt))
				{
					//убрать этот ордер вообще 
					ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_B', buy.__json, function(err, result){
						if (err)
							callBack(1, '[DELETE/Buy(timeout)] ERROR: ' + err + '   ' + result);
						else
						{
sys.log('[EXPIRE] Order ' + buy.t +'  '+buy.f[0]+'#' + buy._ + ' has expired');
							ordbs.publish(options.redisExpiredOrdersStream, buy._, function(err, result){
								if (err)
									callBack(1, '[DELETE/Buy(Notify timeout)] ERROR: ' + err + '   ' + result);
								else
									callBack(null);	
							});	
						}
					});
				}
				else
					callBack(null);
			}],
			function(err, results){
				if (!err)
				{
					return true;
				}			
			}
		);	
	}
	
	//дополнительная проверка 
	if (((sell.c != 0) && (sell.c <= _nowDt)) || ((buy.c != 0) && (buy.c <= _nowDt)))
		return;	
	
	var isMatched = false;
	
	//обе заявки лимитные 
	if ((sell.f[0] == 'L') && (buy.f[0] == 'L')) 
	{
		if (( sell.p <= buy.p ) || ( buy.p >= sell.p ))
		{
			isMatched = true;
		}
	}
	else
	if ((sell.f[0] == 'L') && (buy.f[0] == 'M'))
	{
//sys.puts('\n\n\n\n' + eye.inspect( [ sell, buy ] ) + '\n\n\n\n');		
		buy.p = sell.p;
		buy.s = Number( buy.p * buy.v );
			
		isMatched = true;	
	}
	else
	if ((sell.f[0] == 'M') && (buy.f[0] == 'L'))
	{
		//мы матчим с топ-оф-бук покупок 
//sys.puts('\n\n\n\n' + eye.inspect( [ sell, buy ] ) + '\n\n\n\n');		
		sell.p = buy.p;
		sell.s = Number( sell.p * sell.v );
		
		isMatched = true;		
	}
	else
	if ((sell.f[0] == 'M') && (buy.f[0] == 'M'))
	{
//sys.puts('\n\n\n\n\n\n\n\n ========== Market/Market orders!!! ==========\n\n\n\n\n\n\n\n');
//sys.puts(eye.inspect([sell, buy]));
//sys.puts('\n\n');
		//самая тяжелая ситуация - у нас два маркет-ордера 
		//тогда выбираем для каждого стакана первую не 0 цену и матчим с ней 
		async.parallel([
			function(callBack){
				//для котировки buy выберем топ от sell-очереди 
				ordbs.zrangebyscore([
					options.redisOrderbookPrefix + buy.a.toUpperCase() + '_S',
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
					options.redisOrderbookPrefix + buy.a.toUpperCase() + '_B',
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
					var _best_sell = result[0], _best_buy = result[1];
//sys.puts('\n\n============= Matched by best ==========\n\n');
//sys.puts(eye.inspect([_best_sell, _best_buy]));
//sys.puts('\n\n');					
					if (!_.isEmpty(_best_sell))
					{
						//матчим наш бай с этим селлом 
						buy.p = _best_sell.p;
						buy.s = Number( buy.p * buy.v );
sys.log('[Matched] BUY market matched  => ' + 	_best_sell.p + '/' + buy.v);						
						//теперь пойдем дальше 
						async.parallel([
							function(callBack){				
								ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_B', buy.__json, function(err, result){
									if (err)
										callBack(1, '[DELETE/Buy] ERROR: ' + err + '   ' + result);
									else
										callBack(null);
								});
							},
							function(callBack){	
								ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_S', _best_sell.__json, function(err, result){
									if (err)
										callBack(1, '[DELETE/Sell(best)] ERROR: ' + err + '   ' + result);
									else
										callBack(null);
								});
							}],
							function(err, result){
								if (!err)
								{
									emitter.emit('MQE_matchOrder', _best_sell, buy);
								}
							}
						);						
					}
					
					//теперь вторую пару 
					if (!_.isEmpty(_best_buy))
					{
						//матчим наш бай с этим селлом 
						sell.p = _best_buy.p;
						sell.s = Number( sell.p * sell.v );
sys.log('[Matched] SELL market matched  => ' + 	_best_buy.p + '/' + sell.v);							
						//теперь пойдем дальше 
						async.parallel([
							function(callBack){				
								ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_B', _best_buy.__json, function(err, result){
									if (err)
										callBack(1, '[DELETE/Buy(best)] ERROR: ' + err + '   ' + result);
									else
										callBack(null);
								});
							},
							function(callBack){	
								ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_S', sell.__json, function(err, result){
									if (err)
										callBack(1, '[DELETE/Sell] ERROR: ' + err + '   ' + result);
									else
										callBack(null);
								});
							}],
							function(err, result){
								if (!err)
								{
									emitter.emit('MQE_matchOrder', sell, _best_buy);
								}
							}
						);						
					}
				}
			});
	
		//дальше оно будет матчится паралельно и независимо
		isMatched = false;
	}
	
	if (isMatched == false)
	{
		//sys.log(' === Empty Book === ');	
		return;
	}
	
	//матчим 
	async.parallel([
		function(callBack){				
				ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_B', buy.__json, function(err, result){
					if (err)
						callBack(1, '[DELETE/Buy] ERROR: ' + err + '   ' + result);
					else
						callBack(null);
				});
		},
		function(callBack){	
				ordbs.zrem(options.redisOrderbookPrefix + a.toUpperCase() + '_S', sell.__json, function(err, result){
					if (err)
						callBack(1, '[DELETE/Sell] ERROR: ' + err + '   ' + result);
					else
						callBack(null);
				});
		}],
		function(err, result){
			if (!err)
			{
				emitter.emit('MQE_matchOrder', sell, buy);
			}
		}
	);	
});

var _lastMatchesTime = []; //счетчик среднего матчинга 
//этот метод матчит две котировки конкретные
emitter.addListener('MQE_matchOrder', function(sell, buy){
	//match! 
	//sys.log('[MATCH] Sell: ' + sell.p + '/' + sell.v + '  <===>  ' + buy.p + '/' + buy.v + ': Buy');
	
	//теперь посмотрим как мы матчим
	//sys.log('[ORDER] Sell: ' + sell.p + ' / ' + sell.v); //покупка 
	//sys.log('[ORDER] Buy: ' + buy.p + ' / ' + buy.v);    //продажа

	var t1 = process.hrtime();
	
	var p_match = null;
	var v_match = null;
	
	//вычислим прайс матчинг 
	if ((buy.p >= sell.p) || (sell.p <= buy.p))
	{
		p_match = sell.p;	
	}
	
	//
	
	//теперь обьем 
	if (buy.v >= sell.v)
	{
		v_match = sell.v;
	}
	else
	if (sell.v >= buy.v)
	{
		v_match = buy.v;
	}
	sys.log('[ORDER/MATCH] Finish Price matched: ' + p_match + ', volume: ' + v_match);
	//sys.log('[ORDER] Finish lot Volume matched: ' + v_match);
	
	//теперь сформировать команды с указанием исполнения ордеров и посмотреть, остались ли ордера 
	
	//запишем в ордер
	var matchAt = new Date().getTime();
	
	sell.matched = {
		matchBy : buy._,
		matchP  : p_match,
		matchV  : v_match,
		matchAt : matchAt
	};
	
	buy.matched = {
		matchBy : sell._,
		matchP  : p_match,
		matchV  : v_match,
		matchAt : matchAt
	};
	
	async.parallel([
		function(callBack){
			//теперь отправим эти заявки на исполнение 
			ordbs.publish(options.redisMatchedOrdersStream, JSON.stringify( sell ), function(err, data){
				//для случая, когда на той стороне никто не принимает (временно отвалился, например, акаунт сервере) 
				if ((err) || (data < 1))
				{
					//sys.log('[ERROR] No one readers for AcceptedOrdersStream. Orders queued');
					
					ordbs.rpush(options.redisMatchedOrdersQueue, JSON.stringify( sell ), function(err, data){
						if (!err)
							callBack(null);
						else
							callBack(err, data);
					});
				}
				else
				{				
					if (!err)
						callBack(null);
					else
						callBack(err, data);
				}
			});
		},
		function(callBack){
			//теперь отправим эти заявки на исполнение 
			ordbs.publish(options.redisMatchedOrdersStream, JSON.stringify( buy ), function(err, data){
				if ((err) || (data < 1))
				{
					//sys.log('[ERROR] No one readers for AcceptedOrdersStream. Orders queued');
					
					ordbs.rpush(options.redisMatchedOrdersQueue, JSON.stringify( sell ), function(err, data){
						if (!err)
							callBack(null);
						else
							callBack(err, data);
					});
				}
				else
				{				
					if (!err)
						callBack(null);
					else
						callBack(err, data);
				}
			});
		},
		function(callBack){
			//теперь посмотреть, что осталось от заявок (по обьему)
			//продали меньше, чем обьем заявки 
			if (sell.v > v_match)
			{
				delete sell.matched;
				delete sell.__json;
				
				//обновим обьем 
				sell.v = sell.v - v_match;
								
				if (sell.f[0] == 'M')
				{
					sell.p = 0;
					sell.s = 0;
				}
				else
					sell.s = Number( sell.p * sell.v );
				
				//теперь заявка со скорректированным обьемом доступа снова будет 
				ordbs.publish(options.redisOrdersStream, sell, function(err, data){
					if (!err)
					{
						sys.log('[ORDER/Correct] Order '+sell.f[0]+'#' + sell._ + '/S is corrected by volume: ' + sell.v + '(accept: ' + v_match + ')');
						callBack(null);
					}
					else
						callBack(err, data);
				});				
			}
			else
				callBack(null);
		},
		function(callBack){
			//купили меньше, чем хотели 
			if (v_match < buy.v)
			{
				delete buy.matched;
				delete buy.__json;
				
				//обновим обьем 
				buy.v = buy.v - v_match;				
				
				if (buy.f[0] == 'M')
				{
					buy.p = 999999999;
					buy.s = 0;
				}
				else
					buy.s = Number( buy.p * buy.v );
				
				//теперь заявка со скорректированным обьемом доступа снова будет 
				ordbs.publish(options.redisOrdersStream, buy, function(err, data){
					if (!err)
					{
						sys.log('[ORDER/Correct] Order '+buy.f[0]+'#' + sell._ + '/B is corrected by volume: ' + sell.v + '(accept: ' + v_match + ')');
						callBack(null);
					}
					else
						callBack(err, data);
				});	
			}
			else
				callBack(null);
		}],
		function(err, result){
			
			//var diff = ;
			_lastMatchesTime.push(process.hrtime(t1));

			//sys.log('[DEBUG] Match two order by '+(diff[0] * 1e9 + diff[1])+' ns.');
		}
	);
			
	
	//sys.log('[DEBUG] Match two order by '+(diff[0] * 1e9 + diff[1])+' ns.');
	
});


//выборка данных 
emitter.addListener('MQE_selectTopBook', function(a){
	if (typeof(a) == 'undefined')	a = 'BTC/USD';
	
	//var _book = {'s':null,'b':null};
//var time1 = process.hrtime();	
	//получить топ буков: для продажи - минимум цены, для покупки: максимум 
	async.parallel([
		function(callBack){
			ordbs.zrange([
				options.redisOrderbookPrefix + a.toUpperCase() + '_S',
				0,
				options.matchAtOnceOrders-1], 
			function(err, data){
				if ((!err) && (data.length > 0))
				{
					//поскольку мы можем выбирать несколько котировок, обойдемся одним вызовом JSON.parse 
					var _data = JSON.parse('[' + data.join(',') + ']');
					
					_.each(_data, function(x, i){
						_data[i]['__json'] = data[i];
					});
					
					callBack(null, _data );
				}
				else
					callBack(1, null );
			});
		},
		
		function(callBack){
			ordbs.zrevrange([
				options.redisOrderbookPrefix + a.toUpperCase() + '_B',
				0,
				options.matchAtOnceOrders-1], 
			function(err, data){
				if ((!err) && (data.length > 0))
				{
					var _data = JSON.parse('[' + data.join(',') + ']');
					
					_.each(_data, function(x, i){
						_data[i]['__json'] = data[i];
					});			
					
					callBack(null, _data );
				}
				else
					callBack(1, null);
			});
		}
	],
	function(err, results){
		if (!err)
		{
			emitter.emit('MQE_matchEngine', a, results[0][0], results[1][0]);
		}		
	});
});

//проверка на експайринг ордеров 
emitter.addListener('MQE_expireOrders', function(a){
	//мы выбираем из сета все ордера, которуе уже проекспайрены 
	var _now = new Date().getTime();
	
	//выбрать все, кто равен или меньше текущего 
	ordbs.zrevrangebyscore([
					options.redisOrderExpiresSet + a.toUpperCase(),
					'(' + _now,
					'(0',
					'WITHSCORES'], 
		function(err, data){
			if ((!err) && (data.length > 0))
			{
				sys.log('[EXPIRE] To expired: ' + data.length + ' orders');
				
				sys.puts('\n' + eye.inspect( data ) + '\n');
			
			}
			else
			{
				sys.puts('\n================\n' + eye.inspect(err) + '\n' + eye.inspect(data) + '\n==================================\n');
			
			
			}
		}
	);
});

//генерация фейковой котировки 
emitter.addListener('MQE_generateTestQuote', function(){

	//генерируем тестовую котировку 
	var q = {
		_ : '', 
		a : 'BTC/USD',
		d : new Date().getTime(),
		t : 's', 
		p : 0, 
		v : 1,
		s : 1,
		x : 0,
		c : 0,
		f : 'P00000000'
	};
	
	var _t = process.hrtime()[1];
		//выравниваем по длине 
		if (_t < 100)
			_t = _t + 100;
	
	q._ = q.d + '' + _t;
	
	if (Math.random() > 0.5)
		q.t = 'b';
	
	q.p = Number(Number( Math.random() ).toFixed(3));   //.replace("'",'');
	q.v = _.random(1, 1000);
	q.s = Number( q.p * q.v ); //.toFixed(9).replace("'",'');
	
	//для маркет-ордера цена 0 
	
	if (Math.random() > 0.9)
	{
		q.f = 'M' + q.f;
		
		if (q.t == 'b')
			q.p = 999999999;
		else
			q.p = 0;	
		
		q.s = 0;
	}
	else
		q.f = 'L' + q.f;
	
	//тестируем котировко с лайфтаймом 
	if (Math.random() > 0.8)
	{
		q.c = new Date().getTime() + _.random(1, 600);
	}

	test.publish( options.redisOrdersStream, JSON.stringify( q )); 

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
sys.log(' ===== Setting up runtime ======= ');
//===========================================================================================================


//== Test 
var test = redis.createClient(options.redisPort, options.redisHost, {
		parser: "javascript"
	});
	
	test.on("error", function (err){
		sys.log("[ERROR] Test Redis error " + err);
	});
	
	test.on("connect", function (err){
		sys.log('[OK] Connected to Redis-server at options.redisHost');
	});

setInterval(function(){
	sys.log('================== Gen +500 orders ========================');
	for (var i = 0; i < 1000; i++)
	{
		emitter.emit('MQE_generateTestQuote');
	}

}, 15000);

//emitter.emit('MQE_generateTestQuote');



setInterval(function(){
	
	_.each(options.assets, function(x){
		if (x.trade == 'open')
		{
			emitter.emit('MQE_selectTopBook', x.code);
		}	
	});

}, options.orderMatchInterval);

//таймер експайринга 


setInterval(function(){
	
	_.each(options.assets, function(x){
		if (x.trade == 'open')
		{
			emitter.emit('MQE_expireOrders', x.code);
		}	
	});

}, options.orderExpaireInterval);


//ставим генерацию лучшей котировки 
setInterval(function(){
	emitter.emit('MQE_publishAllBestQuote');	
}, options.bestQuotePublishInterval);




setInterval(function(){
	async.parallel([
					function(callBack){				
						ordbs.zcount(options.redisOrderbookPrefix + 'BTC/USD_B', '-inf','+inf', function(err, result){
							if (err)
								callBack(1, result);
							else
								callBack(null, result);
						});
					},
					function(callBack){	
						ordbs.zcount(options.redisOrderbookPrefix + 'BTC/USD_S', '-inf','+inf', function(err, result){
							if (err)
								callBack(1, result);
							else
								callBack(null, result);
						});
					}],
					function(err, result){
						if (!err)
						{
							sys.log('==== Orderbook depth =====\n\n\n');
							sys.log(' SELL: ' + result[1] + ',  BUY: ' + result[0]);
							sys.log('\n\n');
							
							var _lt = _.clone(_lastMatchesTime);
								_lastMatchesTime = [];
								
							var _s = 0;
							
							_.each(_lt, function(diff){
								_s = _s + Math.ceil(diff[0] * 1e9 + diff[1]);
							});
							
							sys.log('[DEBUG] AvG match two order by '+Number((_s/_lt.length)/1000000).toFixed(3)+' ms.');
							sys.puts('\n\n\n\n\n');						
						}
					}
				);
		


}, 50000);




