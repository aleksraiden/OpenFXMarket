/**  metaExchange Engine **/

var sys 	= require('sys'),
    net		= require('net'),
	fs 		= require('fs'),
	events  = require("events"),
	http 	= require('http'),
	eye 	= require('./lib/eyes'),
	crypto 	= require('crypto'),
	url     = require('url'),
	RJSON   = require('./lib/rjson'),
	redis   = require("./lib/node_redis2/index"),
	async   = require('./lib/async'),
	Buffer 	= require('buffer').Buffer;
	
	var _ = require('./lib/underscore');
		_.mixin(require('./lib/underscore.string'));



var emitter = new events.EventEmitter();
var __HOST__ = 'opentradeengine.com'; //test
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
sys.log(' ====== metaQuote Exchange Engine 0.1 at ' + __HOST__ + ' (Test) ======= ');
sys.puts('\n\n');

// общий обьект настроек сервера
var options = {
	//конфиг для Flash socket server 
	encoding : 'utf8',
	port: 443,
	host : 'exchange.' + __HOST__,
	
	//RedisDB server 
	redisPort: 6379,
	redisHost: 'localhost',
	
	//второй сервер, если есть
	slavePort: 6380,
	slaveHost: 'localhost',
	
	//список инструментов, которые торгуем (или автоматически)
	tradeAssets: [], 
	
	//канал в редисе Pub/Sub для поступающих котировок 
	redisOrdersStream: 'MQE_ORDERS_CHANNEL', 
	//управляющий канал (команды, отмены и т.п.)
	redisControlsStream: 'MQE_CONTROLS_CHANNEL',
	//канал для процессинга ордеров - сюда идут ордера, которые можно матчить 
	redisMatchedOrdersStream: 'MQE_MATCHED_ORDERS_CHANNELS',
	//префикс для ордербуков (добавляем код инструмента в верхнем регистре)
	redisOrderbookPrefix: 'MQE_ORDERBOOK_',
	//название ключа для топовой котировки (хеш со всеми последними котировками (топ-оф-бук)
	redisCurrentQuoteHash: 'MQE_LAST_QUOTES',
	//канал, куда паблишим ордера, которые удачно добавлены в очереди
	redisAcceptedOrdersStream: 'MQE_ACCEPTER_ORDERS_CHANNEL',
	//канал, куда сообщаем ид ордеров, которые не прошли 
	redisErroredOrdersStream: 'MQE_ERRORED_ORDERS_CHANNEL',
	//сет для ордеров, которые выбрали для матча, но еще не обработали 
	//redisProcessingOrders: 'MQE_PROCESSED_ORDERS',
	//где храним, по каким инструментам торгуем 
	redisAssetsTradeConfig: 'MQE_ASSETS_CONFIG',
	//стрим для публикации последней котировки 
	redisLastQuoteStream: 'MQE_LAST_QUOTES_CHANNEL',
	
	
	//в каком формате принимать котировки (пока json, потом возможно более эффективный типа messagepack или protobuf)
	defaultOrderFormat: 'json',
	
	//есть ли ограничение на глубину стакана 
	maxOrderbookDepth: 1000000,
	
	//есть ли ограничение на время жизни ордера (0 - нет, пока не будет исполнен или снят)
	maxOrderLifetime: 0,
	
	//интервал чека стакана на предмет матчинга (в миллисекундах)
	orderMatchInterval: 300,
	
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
			asset_code: 'BTC' // код инструмента, который торгуется (на который заключены контракты)
		}	
	],
	
	//локальная копия списка с ошибками
	lastErroredOrderIds:[],
	lastMatchedOrderIds: [], //ид ордеров, которые отменены или сматченные, которые можно убирать 
	maxLocalErrored: 1000000 //сколько максимум храним ошибочных ордеров 
	
	
};

//список инструментов, по которых сейчас идут торги 
var currentOpenTrades = ['BTC/USD'];

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

//If you want to rotate logs, this will re-open the files on sighup
process.addListener("SIGHUP", function(){
  //log.reopen();
 // log_ban.reopen();
  sys.log('Logfile are reopened for SIGHUP signal');
});

	redis.debug_mode = false;
	
	//это выделенное подключение для Pub/Sub
	var pubsub = redis.createClient(options.redisPort, options.redisHost, {
		parser: "javascript"
	});
	
	pubsub.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	pubsub.on("connect", function (err){
		sys.log('[OK] Connected to Redis-server at options.redisHost');
		
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
	var ordbs = redis.createClient(options.redisPort, options.redisHost, {
		parser: "javascript"
	});
	
	ordbs.on("error", function (err){
		sys.log("[ERROR] Redis error " + err);
	});
	
	ordbs.on("connect", function (err){
		ordbs.select(2);
		
		ordbs.info(function(err, data){
			sys.puts( '\n' + sys.inspect( data ) + '\n');
		});
		
		sys.log('[OK] Connected to Redis-server at options.redisHost');
	});
	


function str_replace (search, replace, subject, count) {
  // http://kevin.vanzonneveld.net
  // +   original by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // +   improved by: Gabriel Paderni
  // +   improved by: Philip Peterson
  // +   improved by: Simon Willison (http://simonwillison.net)
  // +    revised by: Jonas Raoni Soares Silva (http://www.jsfromhell.com)
  // +   bugfixed by: Anton Ongson
  // +      input by: Onno Marsman
  // +   improved by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // +    tweaked by: Onno Marsman
  // +      input by: Brett Zamir (http://brett-zamir.me)
  // +   bugfixed by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
  // +   input by: Oleg Eremeev
  // +   improved by: Brett Zamir (http://brett-zamir.me)
  // +   bugfixed by: Oleg Eremeev
  // %          note 1: The count parameter must be passed as a string in order
  // %          note 1:  to find a global variable in which the result will be given
  // *     example 1: str_replace(' ', '.', 'Kevin van Zonneveld');
  // *     returns 1: 'Kevin.van.Zonneveld'
  // *     example 2: str_replace(['{name}', 'l'], ['hello', 'm'], '{name}, lars');
  // *     returns 2: 'hemmo, mars'
  var i = 0,
    j = 0,
    temp = '',
    repl = '',
    sl = 0,
    fl = 0,
    f = [].concat(search),
    r = [].concat(replace),
    s = subject,
    ra = Object.prototype.toString.call(r) === '[object Array]',
    sa = Object.prototype.toString.call(s) === '[object Array]';
  s = [].concat(s);
  if (count) {
    this.window[count] = 0;
  }

  for (i = 0, sl = s.length; i < sl; i++) {
    if (s[i] === '') {
      continue;
    }
    for (j = 0, fl = f.length; j < fl; j++) {
      temp = s[i] + '';
      repl = ra ? (r[j] !== undefined ? r[j] : '') : r[0];
      s[i] = (temp).split(f[j]).join(repl);
      if (count && s[i] !== temp) {
        this.window[count] += (temp.length - s[i].length) / f[j].length;
      }
    }
  }
  return sa ? s : s[0];
}

//=================
//обработка  
emitter.addListener('MQE_newOrder', function(data){
	//по сути, нам ничего особо не нужно - добавить только в ордербук котировку и потом инициировать обновление топов
	
	//надо проверить, разрешены ли торги по инструменту 
	if (!_.inArray(data.a, currentOpenTrades))
	{
		options.lastErroredOrderIds.push( data._ );
		
		//запишем текст ошибки 
		data.__error = 'Error, trade unavalaible';
				
		//оповестим других про ошибочный ордер 
		ordbs.publish(options.redisErroredOrdersStream, data);	
		
		return;
	}	
	
	ordbs.zadd([ 
				//вида: MQE_ORDERBOOK_BTC/USD_S (sell) или MQE_ORDERBOOK_BTC/USD_B (buy)
				options.redisOrderbookPrefix + data.a.toUpperCase() + '_' + data.t.toUpperCase(), //это куда писать 
				data.p,
				data.__json				
				], 
				function(err, response){
				
			if (!err)
			{
				//добавили? вышлем подтверждение 
				ordbs.publish(options.redisAcceptedOrdersStream, data._);
			}
			else
			{
				options.lastErroredOrderIds.push( data._ );
				//запишем текст ошибки 
				data.__error = response;
				
				//оповестим других про ошибочный ордер 
				ordbs.publish(options.redisErroredOrdersStream, data);			
			}
	});	
});




//собственно, сам матчинг енжайн 
emitter.addListener('MQE_matchEngine', function(a, sell, buy){
	//sys.log('==== [MATCH ENGINE] ===== ');
	//sys.puts( eye.inspect( [sell, buy] ) );
	//sys.puts('\n=========================\n');	
    //return;
	//не матчим ордера: lastMatchedOrderIds (лучше удалять через ZREM 
	
	//допустим, пока матчим только limit-ордера, игнорируем обьемы пока что, только цена 
	
	//sys.puts('\n======= WARNING! TEST MODE! ============= \n');
	
	//если хотя купить за цену == или больше, чем лучшая заявка на продажу - матчим оба 
	//если хотят продать за цену == или меньше, чем лучшая заявка на покупку - матчим оба 
	
	//для поддержки ордеров с експайром 
	var _nowDt = new Date().getTime();
		
	if ((sell.c != 0) || (buy.c != 0))
	{
		//var _sell = _.clone(sell), _bid = _.clone(buy);
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
							callBack(null);
					});
				}
			},
			//оповещаем об отмене 
			function(callBack){
				sell.__error = 'Order expired!';
				sys.log('[EXPIRE] Order ' + sell.t + '  '+sell.f[0]+'#' + sell._ + ' has expired');
				
				ordbs.publish(options.redisErroredOrdersStream, sell, function(err, result){
					if (err)
							callBack(1, '[DELETE/Sell(Notify timeout)] ERROR: ' + err + '   ' + result);
						else
							callBack(null);	
				});			
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
							callBack(null);
					});
				}
			},
			//оповещаем об отмене 
			function(callBack){
				buy.__error = 'Order expired!';
				sys.log('[EXPIRE] Order ' + sell.t +'  '+sell.f[0]+'#' + sell._ + ' has expired');
				
				ordbs.publish(options.redisErroredOrdersStream, buy, function(err, result){
					if (err)
							callBack(1, '[DELETE/Buy(Notify timeout)] ERROR: ' + err + '   ' + result);
						else
							callBack(null);	
				});			
			}],
			function(err, results){
				if (!err)
				{
					return true;
				}			
			}
		);	
	}
	
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

var _lastMatchesTime = [];
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
			ordbs.publish(options.redisAcceptedOrdersStream, JSON.stringify( sell ), function(err, data){
				
			sys.puts('\n\n' + eye.inspect(data) + '\n\n');
				
				if (!err)
					callBack(null);
				else
					callBack(err, data);
			});
		},
		function(callBack){
			//теперь отправим эти заявки на исполнение 
			ordbs.publish(options.redisAcceptedOrdersStream, JSON.stringify( buy ), function(err, data){
				if (!err)
					callBack(null);
				else
					callBack(err, data);
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
			//var diff = process.hrtime(time1);
			
			emitter.emit('MQE_matchEngine', a, results[0][0], results[1][0]);
			
			//sys.log('Select match took '+(diff[0] * 1e9 + diff[1])+' nanoseconds');
		}		
	});
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
	
	q._ = q.d + '' + process.hrtime()[1];
	
	if (Math.random() > 0.5)
		q.t = 'b';
	
	q.p = Number(Number( Math.random() ).toFixed(3));   //.replace("'",'');
	q.v = Math.floor(Math.random() * (1000 - 1 + 1)) + 1;
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
		q.c = new Date().getTime() + Math.floor(Math.random() * (600 - 15 + 1)) + 15;	
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
						code: a,
						ask: result[0].p,
						bid: result[1].p,
						timestamp: result[0].d
					};
					
					if (result[1].d > result[0].d)
						bestQuote.timestamp = result[1].d;
					
					var _json = JSON.stingify( bestQuote );
					//теперь публикуем в пабсаб и ложим в хеш последних котировок
					ordbs.hset([options.redisCurrentQuoteHash, a, _json]);
					
					ordbs.publish(options.redisLastQuoteStream, _json);	

sys.puts('\n\n     Last:  ' + bestQuote.ask + ' / ' + bestQuote.bid + '       \n\n');
					
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
sys.log('\n ===== Setting up runtime ======= \n');

//	getPushServerInit();	
		
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

}, 60000);

emitter.emit('MQE_generateTestQuote');

setInterval(function(){
	
	_.each(options.assets, function(x){
		if (x.trade == 'open')
		{
			emitter.emit('MQE_selectTopBook', x.code);
		}	
	});

}, options.orderMatchInterval);


//ставим генерацию лучшей котировки 
setInterval(function(){
	emitter.emit('MQE_publishAllBestQuote');	
}, options.bestQuotePublishInterval);



/*
process.nextTick(function(){
	emitter.emit('MQE_selectTopBook', 'BTC/USD');
});
*/



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




