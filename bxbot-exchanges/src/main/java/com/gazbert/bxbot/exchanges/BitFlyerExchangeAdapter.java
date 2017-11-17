package com.gazbert.bxbot.exchanges;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gazbert.bxbot.exchange.api.ExchangeAdapter;
import com.gazbert.bxbot.exchange.api.ExchangeConfig;
import com.gazbert.bxbot.trading.api.BalanceInfo;
import com.gazbert.bxbot.trading.api.ExchangeNetworkException;
import com.gazbert.bxbot.trading.api.MarketOrderBook;
import com.gazbert.bxbot.trading.api.OpenOrder;
import com.gazbert.bxbot.trading.api.OrderType;
import com.gazbert.bxbot.trading.api.TradingApiException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.oracle.tools.packager.Log;

/**
 * Implementation of the BitFlyer {@link ExchangeAdapter}.
 *
 * <p>
 *     https://lightning.bitflyer.jp/docs?lang=en
 * </p>
 *
 */
public class BitFlyerExchangeAdapter extends AbstractExchangeAdapter implements ExchangeAdapter {

	private static final Logger LOGGER = LogManager.getLogger();
	private static final String BASE_PATH = "http://api.bitflyer.jp";
	private Gson gson = new Gson();

	@Override
	public void init(ExchangeConfig config) {
		Log.info("Initializing BitFlyer Exchange Adapater.");
	}

	@Override
	public String getImplName() {
		return "BitFlyer";
	}

	@Override
	public MarketOrderBook getMarketOrders(String marketId)
			throws ExchangeNetworkException, TradingApiException {
		return null;
	}

	@Override
	public List<OpenOrder> getYourOpenOrders(String marketId)
			throws ExchangeNetworkException, TradingApiException {
		// GET /v1/me/getchildorders

		// build request
		JsonObject parameters = new JsonObject();
		parameters.addProperty("product_code", marketId);
		parameters.addProperty("count", Integer.MAX_VALUE); // TODO: make better
		parameters.addProperty("child_order_state", "ACTIVE");

		// execute that shiet
		String data = parameters.toString();
		LOGGER.info("executing list orders with parameters: " + data);
		try {
			ExchangeHttpResponse get = super.sendNetworkRequest(buildUrl("/v1/me/getchildorders"), "GET", data, null);

			// covert to our response, bitchez
			BitFlyerOrderStatusResponse[] responses = gson.fromJson(get.getPayload(), BitFlyerOrderStatusResponse[].class);

			// map to our model type
			return Stream.of(responses)
					.map(r -> new OpenOrder(
								r.getChildOrderId(),
								r.getChildOrderDate(),
								r.getProductCode(),
								r.getSide(),
								r.getPrice(),
								r.getExecutedSize(),
								r.getSize(),
								r.getPrice())).collect(Collectors.toList());
		} catch (MalformedURLException e) {
			LOGGER.error("error listing BitFlyer orders", e);
			throw new TradingApiException(e.getMessage(), e);
		}
	}

	@Override
	public String createOrder(
			String marketId, OrderType orderType,
			BigDecimal quantity, BigDecimal price)
			throws ExchangeNetworkException, TradingApiException {
		// POST /v1/me/sendchildorder

		// build request
		JsonObject parameters = new JsonObject();
		parameters.addProperty("product_code", marketId);
		parameters.addProperty("child_order_type", "MARKET");  // TODO: support order types!
		parameters.addProperty("side", orderType.toString());
		parameters.addProperty("price", price);
		parameters.addProperty("size", quantity);

		// execute
		String data = parameters.toString();
		LOGGER.info("executing create order with parameters: " + data);
		try {
			ExchangeHttpResponse post = super.sendNetworkRequest(
					buildUrl("/v1/me/sendchildorder"), "POST", data, null);

			// convert to response
			BitFlyerCreateOrderResponse resp = gson.fromJson(post.getPayload(), BitFlyerCreateOrderResponse.class);
			return resp.getChildOrderAcceptanceId();
		} catch (MalformedURLException e) {
			LOGGER.error("error creating BitFlyer order", e);
			throw new TradingApiException(e.getMessage(), e);
		}
	}

	@Override
	public boolean cancelOrder(String orderId, String marketId)
			throws ExchangeNetworkException, TradingApiException {
		// POST /v1/me/cancelchildorder

		// build request
		JsonObject parameters = new JsonObject();
		parameters.addProperty("product_code", marketId);
		parameters.addProperty("child_order_acceptance_id", orderId);

		// execute
		String data = parameters.toString();
		LOGGER.info("executing cancel order with parameters: " + data);

		try {

			ExchangeHttpResponse post = super.sendNetworkRequest(
					buildUrl("/v1/me/cancelchildorder"), "POST", data, null);

			return post.getStatusCode()==200;
		} catch (MalformedURLException e) {
			LOGGER.error("error cancelling BitFlyer order", e);
			throw new TradingApiException(e.getMessage(), e);
		}
	}

	@Override
	public BigDecimal getLatestMarketPrice(String marketId)
			throws ExchangeNetworkException, TradingApiException {
		return null;
	}

	@Override
	public BalanceInfo getBalanceInfo()
			throws ExchangeNetworkException, TradingApiException {
		// GET /v1/me/getbalance

		LOGGER.info("executing balance request");

		try {
			ExchangeHttpResponse get = super.sendNetworkRequest(buildUrl("/v1/me/getbalance"), "GET", null, null);

			BitFlyerBalanceResponse[] responses = gson.fromJson(get.getPayload(), BitFlyerBalanceResponse[].class);

			Map<String, BigDecimal> retAvailable = new HashMap<>();
			Map<String, BigDecimal> retOnHold = new HashMap<>();

			Stream.of(responses)
					.forEach(r -> {
						retAvailable.put(r.getCurrencyCode(), r.getAvailable());
						retOnHold.put(r.getCurrencyCode(), r.getAmount().subtract(r.getAvailable()));
					});

			return new BalanceInfo(retAvailable, retOnHold);
		} catch (MalformedURLException e) {
			LOGGER.error("error getting balance info from BitFlyer", e);
			throw new TradingApiException(e.getMessage(), e);
		}
	}

	@Override
	public BigDecimal getPercentageOfBuyOrderTakenForExchangeFee(String marketId)
			throws TradingApiException, ExchangeNetworkException {
		return getTradingCommission(marketId);
	}

	@Override
	public BigDecimal getPercentageOfSellOrderTakenForExchangeFee(String marketId)
			throws TradingApiException, ExchangeNetworkException {
		return getTradingCommission(marketId);
	}

	/**
	 * BitFlyer /v1/me/gettradingcommission call.
	 *
	 * @param marketId the market id
	 * @return the commision
	 */
	private BigDecimal getTradingCommission(String marketId) throws ExchangeNetworkException, TradingApiException {

		JsonObject requestParams = new JsonObject();
		requestParams.addProperty("product_code", marketId);
		String data = requestParams.toString();
		
		try {
			ExchangeHttpResponse get = super.sendNetworkRequest(
					buildUrl("/v1/me/gettradecommission"), "GET", data, null);

			return new JsonParser().parse(get.getPayload()).getAsJsonObject().getAsBigDecimal();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Build the URL needed to make requests.
	 *
	 * @param path the path
	 * @return a URL
	 * @throws MalformedURLException on exception
	 */
	private URL buildUrl(String path) throws MalformedURLException {
		return new URL(BASE_PATH+path);
	}

	/**
	 * Balance response.
	 *
	 */
	private static class BitFlyerBalanceResponse {

		@SerializedName("currency_code")
		private String currencyCode;

		@SerializedName("amount")
		private BigDecimal amount;

		@SerializedName("available")
		private BigDecimal available;

		public String getCurrencyCode() {
			return currencyCode;
		}

		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}

		public BigDecimal getAmount() {
			return amount;
		}

		public void setAmount(BigDecimal amount) {
			this.amount = amount;
		}

		public BigDecimal getAvailable() {
			return available;
		}

		public void setAvailable(BigDecimal available) {
			this.available = available;
		}
	}

	/**
	 * Create order response.
	 *
	 */
	private static class BitFlyerCreateOrderResponse {

		@SerializedName("child_order_acceptance_id")
		private String childOrderAcceptanceId;

		public String getChildOrderAcceptanceId() {
			return childOrderAcceptanceId;
		}

		public void setChildOrderAcceptanceId(String childOrderAcceptanceId) {
			this.childOrderAcceptanceId = childOrderAcceptanceId;
		}
	}

	/**
	 * Order status response.
	 *
	 */
	private static class BitFlyerOrderStatusResponse {

		@SerializedName("id")
		private Long id;

		@SerializedName("child_order_id")
		private String childOrderId;

		@SerializedName("product_code")
		private String productCode;

		@SerializedName("side")
		private OrderType side;

		@SerializedName("child_order_type")
		private String childOrderType;

		@SerializedName("price")
		private BigDecimal price;

		@SerializedName("average_price")
		private BigDecimal averagePrice;

		@SerializedName("size")
		private BigDecimal size;

		@SerializedName("child_order_state")
		private String childOrderState;

		@SerializedName("expire_date")
		private Date expireDate;

		@SerializedName("child_order_date")
		private Date childOrderDate;

		@SerializedName("child_order_acceptance_id")
		private String childOrderAcceptanceId;

		@SerializedName("outstanding_size")
		private BigDecimal outstandingSize;

		@SerializedName("cancel_size")
		private BigDecimal cancelSize;

		@SerializedName("executed_size")
		private BigDecimal executedSize;

		@SerializedName("total_commission")
		private BigDecimal totalCommission;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getChildOrderId() {
			return childOrderId;
		}

		public void setChildOrderId(String childOrderId) {
			this.childOrderId = childOrderId;
		}

		public String getProductCode() {
			return productCode;
		}

		public void setProductCode(String productCode) {
			this.productCode = productCode;
		}

		public OrderType getSide() {
			return side;
		}

		public void setSide(OrderType side) {
			this.side = side;
		}

		public String getChildOrderType() {
			return childOrderType;
		}

		public void setChildOrderType(String childOrderType) {
			this.childOrderType = childOrderType;
		}

		public BigDecimal getPrice() {
			return price;
		}

		public void setPrice(BigDecimal price) {
			this.price = price;
		}

		public BigDecimal getAveragePrice() {
			return averagePrice;
		}

		public void setAveragePrice(BigDecimal averagePrice) {
			this.averagePrice = averagePrice;
		}

		public BigDecimal getSize() {
			return size;
		}

		public void setSize(BigDecimal size) {
			this.size = size;
		}

		public String getChildOrderState() {
			return childOrderState;
		}

		public void setChildOrderState(String childOrderState) {
			this.childOrderState = childOrderState;
		}

		public Date getExpireDate() {
			return expireDate;
		}

		public void setExpireDate(Date expireDate) {
			this.expireDate = expireDate;
		}

		public Date getChildOrderDate() {
			return childOrderDate;
		}

		public void setChildOrderDate(Date childOrderDate) {
			this.childOrderDate = childOrderDate;
		}

		public String getChildOrderAcceptanceId() {
			return childOrderAcceptanceId;
		}

		public void setChildOrderAcceptanceId(String childOrderAcceptanceId) {
			this.childOrderAcceptanceId = childOrderAcceptanceId;
		}

		public BigDecimal getOutstandingSize() {
			return outstandingSize;
		}

		public void setOutstandingSize(BigDecimal outstandingSize) {
			this.outstandingSize = outstandingSize;
		}

		public BigDecimal getCancelSize() {
			return cancelSize;
		}

		public void setCancelSize(BigDecimal cancelSize) {
			this.cancelSize = cancelSize;
		}

		public BigDecimal getExecutedSize() {
			return executedSize;
		}

		public void setExecutedSize(BigDecimal executedSize) {
			this.executedSize = executedSize;
		}

		public BigDecimal getTotalCommission() {
			return totalCommission;
		}

		public void setTotalCommission(BigDecimal totalCommission) {
			this.totalCommission = totalCommission;
		}
	}
}
