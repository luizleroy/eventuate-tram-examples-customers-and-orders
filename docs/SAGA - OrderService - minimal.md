### Consistência para limite de créditos e resiliência ao Criar Pedidos

O fluxo da Saga se inicia no `OrderService` quando um novo pedido é criado. O método `createOrder` é transacional, garantindo que a persistência da entidade `Order` com o estado `PENDING` e o registro do evento `OrderCreatedEvent` ocorram de forma atômica. A biblioteca Eventuate Tram (ETS) utiliza o padrão *transactional outbox*, inserindo o evento em uma tabela `MESSAGE` no mesmo banco de dados do `Order Service`, em vez de publicá-lo diretamente. Apenas após o commit da transação principal é que o evento será de fato publicado, garantindo a consistência.

```java
// OrderService.java - Início da Saga
@Transactional
public Order createOrder(OrderDetails orderDetails) {
  // 1. Cria a ordem com estado PENDING e o evento OrderCreated
  ResultWithTypedEvents<Order, OrderEvent> orderWithEvents = Order.createOrder(orderDetails);
  Order order = orderWithEvents.getResult();
  
  // 2. Salva a ordem no banco de dados
  orderRepository.save(order);
  
  // 3. Publica o evento de forma transacional (padrão outbox)
  orderEventPublisher.publish(order, orderWithEvents.getEvents());
  
  return order;
}
```

O componente de Captura de Dados de Mudança (CDC) do Eventuate Tram é crucial para a publicação confiável de eventos. Ele opera de forma desacoplada dos serviços, monitorando diretamente o log de transações do banco de dados (como o Binlog do MySQL ou o WAL do PostgreSQL). Ao detectar uma nova entrada na tabela `MESSAGE` (o "outbox"), o serviço de CDC lê essa entrada e a publica em um broker de mensagens, como o Apache Kafka. Essa abordagem é altamente performática e resiliente, pois não sobrecarrega o serviço principal com a lógica de publicação e garante que apenas eventos de transações confirmadas (comitadas) sejam enviados. A compatibilidade com diferentes bancos de dados depende do suporte do conector CDC, sendo uma solução robusta para ambientes que não podem permitir a perda de eventos.

O `Customer Service`, por sua vez, consome o evento `OrderCreatedEvent` através do `OrderEventConsumer`. O método `handleOrderCreatedEvent` invoca a lógica de negócio para reservar o crédito, que, de forma similar, publica eventos de sucesso (`CustomerCreditReservedEvent`) ou falha (`CustomerCreditReservationFailedEvent`) utilizando o mesmo mecanismo transacional. A lógica central reside no método `reserveCredit`, que verifica a disponibilidade de crédito e atualiza o estado do cliente.

```java
// customer-service: CustomerService.java - Lógica de reserva de crédito
public void reserveCredit(long orderId, long customerId, Money orderTotal) {
  // ... (lógica para encontrar o cliente)
  try {
    customer.reserveCredit(orderId, orderTotal);
    // Publica evento de sucesso
    customerEventPublisher.publish(customer, new CustomerCreditReservedEvent(customerId, orderId));
  } catch (CustomerCreditLimitExceededException e) {
    // Publica evento de falha
    customerEventPublisher.publish(customer, new CustomerCreditReservationFailedEvent(customerId, orderId));
  }
}
```

Por fim, a Saga é concluída quando o `Order Service` reage aos eventos do `Customer Service`. O `CustomerEventConsumer` no `Order Service` processa os eventos de resultado. Ao receber `CustomerCreditReservedEvent`, ele invoca `orderService.approveOrder()`, mudando o estado do pedido para `APPROVED`. Caso receba um evento de falha, como `CustomerCreditReservationFailedEvent`, ele chama `orderService.rejectOrder()`, atualizando o estado para `REJECTED`. Esse fluxo garante que o pedido sempre atinja um estado final consistente, demonstrando a resiliência e a consistência eventual que a arquitetura de Sagas proporciona.
