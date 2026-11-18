pragma solidity ^0.8.26;

contract DeliveryLifecycle {
    enum Status { Created, Accepted, InTransit, Delivered, Cancelled }
    struct Delivery {
        uint256 id;
        uint256 orderId;
        address customer;
        address courier;
        uint256 priceCents;
        Status status;
    }
    address public immutable operator;
    uint256 public nextId = 1;
    mapping(uint256 => Delivery) private deliveries;
    mapping(address => bool) public couriers;
    error Unauthorized();
    error MissingDelivery();
    error InvalidTransition();
    error InvalidInput();
    event DeliveryCreated(uint256 indexed id, uint256 orderId, address indexed customer);
    event CourierAssigned(uint256 indexed id, address indexed courier);
    event DeliveryStatusChanged(uint256 indexed id, Status previous, Status current);

    constructor() { operator = msg.sender; }
    modifier onlyOperator() {
        if (msg.sender != operator) revert Unauthorized();
        _;
    }
    function setCourier(address courier, bool enabled) external onlyOperator {
        if (courier == address(0)) revert InvalidInput();
        couriers[courier] = enabled;
    }
    function createDelivery(uint256 orderId, uint256 priceCents) external returns (uint256) {
        if (orderId == 0 || priceCents == 0) revert InvalidInput();
        uint256 id = nextId++;
        deliveries[id] = Delivery(id, orderId, msg.sender, address(0), priceCents, Status.Created);
        emit DeliveryCreated(id, orderId, msg.sender);
        return id;
    }
    function getDelivery(uint256 id) external view returns (Delivery memory) {
        Delivery storage delivery = existing(id);
        return delivery;
    }
    function assignCourier(uint256 id, address courier) external onlyOperator {
        Delivery storage delivery = existing(id);
        if (!couriers[courier]) revert InvalidInput();
        if (delivery.status != Status.Created) revert InvalidTransition();
        delivery.courier = courier;
        transition(delivery, Status.Accepted);
        emit CourierAssigned(id, courier);
    }
    function startDelivery(uint256 id) external {
        Delivery storage delivery = existing(id);
        if (msg.sender != delivery.courier || !couriers[msg.sender]) revert Unauthorized();
        if (delivery.status != Status.Accepted) revert InvalidTransition();
        transition(delivery, Status.InTransit);
    }
    function completeDelivery(uint256 id) external {
        Delivery storage delivery = existing(id);
        if (msg.sender != delivery.courier || !couriers[msg.sender]) revert Unauthorized();
        if (delivery.status != Status.InTransit) revert InvalidTransition();
        transition(delivery, Status.Delivered);
    }
    function cancelDelivery(uint256 id) external {
        Delivery storage delivery = existing(id);
        if (msg.sender != delivery.customer) revert Unauthorized();
        if (delivery.status != Status.Created && delivery.status != Status.Accepted) revert InvalidTransition();
        transition(delivery, Status.Cancelled);
    }
    function existing(uint256 id) private view returns (Delivery storage delivery) {
        delivery = deliveries[id];
        if (delivery.id == 0) revert MissingDelivery();
    }
    function transition(Delivery storage delivery, Status next) private {
        Status previous = delivery.status;
        delivery.status = next;
        emit DeliveryStatusChanged(delivery.id, previous, next);
    }
}