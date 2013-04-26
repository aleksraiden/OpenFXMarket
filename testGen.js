/**  openFXMarket - test quotes Engine **/

var sys 	= require('sys'),
    net		= require('net'),
	events  = require("events"),
	eye 	= require('./lib/eyes'),
	crypto 	= require('crypto'),
	redis   = require("./lib/node_redis2/index"),
	async   = require('./lib/async'),	
	uuid    = require('./lib/uuid'), //для генерации ид ордеров используем https://github.com/OrangeDog/node-uuid
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
sys.puts(' ====== test generator for exchange Engine 0.1 at ' + __HOST__ + ' (Dev version) ======= ');
sys.puts('\n\n');

// общий обьект настроек сервера
var options = {
	
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
		
	//в каком формате принимать котировки (пока json, потом возможно более эффективный типа messagepack или protobuf)
	defaultOrderFormat: 'json',
	
	//с каким интервалом генерировать, мс.
	orderGenerateInterval: 1000,
	
	//сколько парралельно создавать ордеров 
	parralelOrders: 3, 
	
	//сколько в одной серии генерировать паралельно 
	ordersAtParralelSeries: 10, 
	
	//список инструментов, по которому генерируем 
	assetsList: ['BTC/USD']
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
	});
	
	//храним ид таймеров разных 
	var _timers = {};
	
//=================

//генерация фейковой котировки 
emitter.addListener('MQE_generateOneQuote', function(a){

	//генерируем тестовую котировку 
	var q = {
		_ : '', 
		a : a.toUpperCase(),
		d : new Date().getTime(),
		t : 's', 
		p : 0, 
		v : 1,
		s : 1,
		x : 0,
		c : 0,
		f : 'P00000000'
	};

	q._ = uuid.v4({rng: uuid.nodeRNG});
	//теперь id ордера это уникальный UUID (скорее всего v4)
	
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
	if (Math.random() > 0.75)
	{
		q.c = new Date().getTime() + (_.random(1, 600) * 1000);
	}
//sys.puts( eye.inspect( q ) );
	pubsub.publish( options.redisOrdersStream, JSON.stringify( q )); 
	
	return true;
});

//метод проходит по всем котировкам и показывает лучшие
emitter.addListener('MQE_generateTestQuote', function(){
	_.each(options.assetsList, function(x){
		//sys.log(' Gen to asset: ' + x);
		emitter.emit('MQE_generateOneQuote', x);			
	});
});


//===========================================================================================================
sys.log(' ===== Setting up runtime ======= ');
//===========================================================================================================

var testGen = setInterval(function(){
	
	sys.log('[TEST] Generate ' + options.ordersAtParralelSeries + ' orders at ' + options.parralelOrders + ' series parralel');    
	
	var _pTasks = [];
	
	for (var i = 0; i < options.parralelOrders; i++)
	{
		_pTasks.push( function(callBack){
			for (var j = 0; j < options.ordersAtParralelSeries; j++)
			{
				emitter.emit('MQE_generateTestQuote');
			}
			
			callBack(null);
		} );
	}
	
	if (_pTasks.length > 0)
	{
		async.parallel( _pTasks, function(err, data){
			if (!err)
				sys.log('[TEST] test quotes generate OK!');
		});
	}

}, options.orderGenerateInterval);



