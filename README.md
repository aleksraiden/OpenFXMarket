OpenFXMarket
==================

Fast abstract market exchange (orderbook and order matching engine)

Проект получил свое имя и домен: openfxmarket.com (сайт скоро будет). 


Data Format Spec
=================

Order - базовый элемент торговой системы. Содержит обезличенные данные котировок и всю информацию по сделке. 
Имеет вид упрощенного JSON-документа следующего формата:

  * _ - идентификатор заявки, который единственный служит для определения заявки во внешней системе биржи. Целое число.
  * a - код инструмента, по которому идут торги (строка, 6 - 9 символов)
  * d - дата и время котировки, UTC с миллисекундами (целое число)
  * t - тип котировки: sell|buy, строка 1 символ (s|b)
  * p - цена за 1 единицу инструмента (число, 9 знаков после запятой, в виде целого числа)
  * v - объем сделки в единицах инструментов (число, 9 знаков после запятой, в виде целого числа)
  * s - размер ордера - обьем сделки * цена инструмента (число, 9 знаков после запятой, в виде целого числа)
  * x - коммисия, в случае, если она индивидуальная. Это конечная сумма, отнимаемая от размера ордера при исполнении
  * c - время отмены ордера, в случае, если установлен, заявка будет исполнена только если время меньше указанного. UTC с миллисекундами.
  * f - флаги сделки (строка 10 символов, каждый символ - отдельный флаг, неиспользуемые - 0). 
    * Первый символ: тип ордера (L - лимит ордер, M - маркет ордер, тогда цена или обьем могут быть не указаны, S - стоп-лосс (пока зарезервирован)
    * Второй символ: порядок исполнения ордера - F|P - только полностью, или ничего (F), P - ордер может быть удовлетворен частично (на сумму, не менее, чем размер коммисии)


AccountEngine
=================

Сервис аккаунтов - это также абстрагированный механизм учета сделок и расчета состояния торговых аккаунтов. 
Он независим от торговой системы и учитывает лишь отсылаемые ему ордера. Также сервис не ведет привязки к конкретным клиентам, 
он лишь расчитывает баланс, согласно указаниями. Основной сервер должен использовать его данные для предварительной проверки,
перед постановкой ордера в торговую систему. 

Основной сервиса аккаунтов является, как не сложно понять: аккаунт. Это структура счетов, а также истории операций по каждому счету. 
Счета имеют стандартный формат и номинированы в одной из валют (активов), которыми ведутся торги. Для системы это лишь абстрактные коды, 
здесь нет никакой привязки к валютам. Также задачей сервиса является обновление общей таблицы балансов аккаунтов и удержание с каждой операции комиссии, 
задаваемой в конфигурации торговой системы по определенной паре инструментов. 

В общем случае, сервис может быть запущен или остановлен (или кратковременно прерван), при этом не полученные ордера на исполнение будут 
накоплены в специальной очереди - MQE_MATCHED_ORDERS. Сервис !ОБЯЗАН! после запуска проверить эту очередь, и выполнить все полученные оттуда ордера в порядке их следования 
ДО того, как начать обрабатывать новые сообщения. При этом он может быть подключенным и готовым принимать запросы, но очереднсоть выполнения не должна быть нарушена. 

Для автоматической проверки баланса запускается периодическая проверка, при этом общая сумма всех счетов (раздельно по каждому инструменту) должна быть равной 0.
