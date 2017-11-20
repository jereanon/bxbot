package com.gazbert.bxbot.exchanges;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gazbert.bxbot.exchange.api.ExchangeAdapter;
import com.gazbert.bxbot.exchange.api.ExchangeConfig;
import com.gazbert.bxbot.trading.api.BalanceInfo;
import com.gazbert.bxbot.trading.api.ExchangeNetworkException;
import com.gazbert.bxbot.trading.api.Market;
import com.gazbert.bxbot.trading.api.MarketOrder;
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
 * TODO: need to use authenticated calls where appropriate as well as testing
 *
 */
public class BitFlyerExchangeAdapter extends AbstractExchangeAdapter implements ExchangeAdapter {

	private static final Logger LOGGER = LogManager.getLogger();
	private static final String BASE_PATH = "http://api.bitflyer.jp";
	private String authKey;
	private String authSecret;
	private Mac mac;

	private Gson gson = new Gson();
	private boolean initializedMACAuthentication = false;

	@Override
	public void init(ExchangeConfig config) {
		Log.info("Initializing BitFlyer Exchange Adapater.");
		this.authKey = config.getAuthenticationConfig().getItem("ACCESS-KEY");
		this.authSecret = config.getAuthenticationConfig().getItem("ACCESS-SECRET");

		// init the security bullshit
		try {
			mac = Mac.getInstance("HmacSHA256");
			byte[] base64DecodedSecret = Base64.getDecoder().decode(authSecret);
			SecretKeySpec keySpec = new SecretKeySpec(base64DecodedSecret, "HmacSHA256");
			mac.init(keySpec);
			initializedMACAuthentication = true;
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error("Unable to initialize security.", e);
			throw new IllegalArgumentException("unable to auth.", e);
		} catch (InvalidKeyException e) {
			LOGGER.error("Unable to initialize security.", e);
			throw new IllegalArgumentException("unable to auth.", e);
		}
	}

	@Override
	public String getImplName() {
		return "BitFlyer";
	}

	@Override
	public MarketOrderBook getMarketOrders(String marketId)
			throws ExchangeNetworkException, TradingApiException {
		// v1/getboard

		// request
		JsonObject parameters = new JsonObject();
		parameters.addProperty("product_code", marketId);

		// execute
		String data = parameters.toString();
		LOGGER.info("executing getmarketorders with parameters: " + data);
		try {
			ExchangeHttpResponse get = super.sendNetworkRequest(buildUrl("/v1/getboard"), "GET", data, null);

			// convert to our response
			BitFlyerOrderBookResponse response = gson.fromJson(get.getPayload(), BitFlyerOrderBookResponse.class);

			List<MarketOrder> buyOrders = response.getBids()
					.stream()
					.map(o -> new MarketOrder(OrderType.BUY,
							o.getPrice(), o.getSize(),
							o.getPrice().multiply(o.getSize())))
					.collect(Collectors.toList());

			List<MarketOrder> sellOrders = response.getAsks()
					.stream()
					.map(o -> new MarketOrder(OrderType.SELL,
							o.getPrice(), o.getSize(),
							o.getPrice().multiply(o.getSize())))
					.collect(Collectors.toList());

			return new MarketOrderBook(marketId, sellOrders, buyOrders);
		} catch (MalformedURLException e) {
			LOGGER.error("error listing BitFlyer board", e);
			throw new TradingApiException(e.getMessage(), e);
		}
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
			// TODO: PROTECTED
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
		// GET /v1/getticker
		// request
		JsonObject parameters = new JsonObject();
		parameters.addProperty("product_code", marketId);

		// execute
		String data = parameters.toString();
		LOGGER.info("executing getticket with parameters: " + data);
		try {
			ExchangeHttpResponse get = super.sendNetworkRequest(buildUrl("/v1/getticker"), "GET", data, null);

			// convert to our response
			BitFlyerTickerResponse response = gson.fromJson(get.getPayload(), BitFlyerTickerResponse.class);

			return response.getBestBid();
		} catch (MalformedURLException e) {
			LOGGER.error("error listing BitFlyer board", e);
			throw new TradingApiException(e.getMessage(), e);
		}

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
					buildUrl("/v1/me/gettradingcommission"), "GET", data, null);

			return new JsonParser().parse(get.getPayload()).getAsJsonObject().getAsBigDecimal();
		} catch (MalformedURLException e) {
			LOGGER.error("error getting trading commissions.", e);
			throw new TradingApiException(e.getMessage(), e);
		}
	}

	/**
	 * Wrap a request so that we can easily add the
	 * appropriate authentication header information.
	 *
	 * <p>
	 *      ACCESS-KEY: api key
	 *      ACCESS-TIMESTAMP: unix timestamp
	 *      ACCESS-SIGN: HMAC-SHA256 signature using ACCESS-TIMESTAMP, HTTP-METHOD, REQUEST PATH, and REQUEST-BODY
	 *      linked together as a character string.
	 * </p>
	 *
	 * @param httpMethod the http method
	 * @param apiMethod the api method
	 * @param params any extra params to add to the request
	 */
	private ExchangeHttpResponse sendAuthenticatedRequest(
			String httpMethod, String apiMethod, Map<String, String> params) throws
			ExchangeNetworkException, TradingApiException {

		if (!initializedMACAuthentication) {
			final String errorMsg = "MAC Message security layer has not been initialized.";
			LOGGER.error(errorMsg);
			throw new IllegalStateException(errorMsg);
		}

		try {

			if (params == null) {
				// create empty map for non-param API calls
				params = new HashMap<>();
			}

			// Get UNIX time in secs
			final String timestamp = Long.toString(System.currentTimeMillis() / 1000);

			// Build the request
			final String invocationUrl;
			String requestBody = "";

			switch (httpMethod) {

				case "GET":
					LOGGER.debug(() -> "Building secure GET request...");
					// Build (optional) query param string
					final StringBuilder queryParamBuilder = new StringBuilder();
					for (final Map.Entry<String, String> param : params.entrySet()) {
						if (queryParamBuilder.length() > 0) {
							queryParamBuilder.append("&");
						}
						queryParamBuilder.append(param.getKey());
						queryParamBuilder.append("=");
						queryParamBuilder.append(param.getValue());
					}

					final String queryParams = queryParamBuilder.toString();
					LOGGER.debug(() -> "Query param string: " + queryParams);

					if (params.isEmpty()) {
						invocationUrl = BASE_PATH + apiMethod;
					} else {
						invocationUrl = BASE_PATH + apiMethod + "?" + queryParams;
					}
					break;

				case "POST":
					LOGGER.debug(() -> "Building secure POST request...");
					invocationUrl = BASE_PATH + apiMethod;
					requestBody = gson.toJson(params);
					break;

				case "DELETE":
					LOGGER.debug(() -> "Building secure DELETE request...");
					invocationUrl = BASE_PATH + apiMethod;
					break;

				default:
					throw new IllegalArgumentException("Don't know how to build secure [" + httpMethod + "] request!");
			}

			// Build the signature string
			final String signatureBuilder = timestamp + httpMethod.toUpperCase() +
					"/" +
					apiMethod +
					requestBody;

			// Sign the signature string and Base64 encode it
			mac.reset();
			mac.update(signatureBuilder.getBytes("UTF-8"));
			final String signature = DatatypeConverter.printBase64Binary(mac.doFinal());

			// Request headers required by Exchange
			final Map<String, String> requestHeaders = new HashMap<>();
			requestHeaders.put("Content-Type", "application/json");
			requestHeaders.put("ACCESS-KEY", authKey);
			requestHeaders.put("ACCESS-SIGN", signature);
			requestHeaders.put("ACCESS-TIMESTAMP", timestamp);

			final URL url = new URL(invocationUrl);
			return sendNetworkRequest(url, httpMethod, requestBody, requestHeaders);

		} catch (MalformedURLException | UnsupportedEncodingException e) {
			final String errorMsg = e.getMessage();
			LOGGER.error(errorMsg, e);
			throw new TradingApiException(errorMsg, e);
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
	 * Bitflyer ticker response.
	 *
	 */
	private static class BitFlyerTickerResponse {

		@SerializedName("product_code")
		private Market market;

		@SerializedName("timestamp")
		private String time;

		@SerializedName("tick_id")
		private long tickId;

		@SerializedName("best_bid")
		private BigDecimal bestBid;

		@SerializedName("best_ask")
		private BigDecimal bestAsk;

		@SerializedName("best_bid_size")
		private BigDecimal bestBidSize;

		@SerializedName("best_ask_size")
		private BigDecimal bestAskSize;

		@SerializedName("total_bid_depth")
		private BigDecimal totalBidDepth;

		@SerializedName("total_ask_depth")
		private BigDecimal totalAskDepth;

		@SerializedName("ltp")
		private long ltp;

		@SerializedName("volume")
		private BigDecimal volume;

		@SerializedName("volume_by_product")
		private BigDecimal volumeByProduct;

		public Market getMarket() {
			return market;
		}

		public void setMarket(Market market) {
			this.market = market;
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

		public long getTickId() {
			return tickId;
		}

		public void setTickId(long tickId) {
			this.tickId = tickId;
		}

		public BigDecimal getBestBid() {
			return bestBid;
		}

		public void setBestBid(BigDecimal bestBid) {
			this.bestBid = bestBid;
		}

		public BigDecimal getBestAsk() {
			return bestAsk;
		}

		public void setBestAsk(BigDecimal bestAsk) {
			this.bestAsk = bestAsk;
		}

		public BigDecimal getBestBidSize() {
			return bestBidSize;
		}

		public void setBestBidSize(BigDecimal bestBidSize) {
			this.bestBidSize = bestBidSize;
		}

		public BigDecimal getBestAskSize() {
			return bestAskSize;
		}

		public void setBestAskSize(BigDecimal bestAskSize) {
			this.bestAskSize = bestAskSize;
		}

		public BigDecimal getTotalBidDepth() {
			return totalBidDepth;
		}

		public void setTotalBidDepth(BigDecimal totalBidDepth) {
			this.totalBidDepth = totalBidDepth;
		}

		public BigDecimal getTotalAskDepth() {
			return totalAskDepth;
		}

		public void setTotalAskDepth(BigDecimal totalAskDepth) {
			this.totalAskDepth = totalAskDepth;
		}

		public long getLtp() {
			return ltp;
		}

		public void setLtp(long ltp) {
			this.ltp = ltp;
		}

		public BigDecimal getVolume() {
			return volume;
		}

		public void setVolume(BigDecimal volume) {
			this.volume = volume;
		}

		public BigDecimal getVolumeByProduct() {
			return volumeByProduct;
		}

		public void setVolumeByProduct(BigDecimal volumeByProduct) {
			this.volumeByProduct = volumeByProduct;
		}
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
	 * Order book? A collection of bids, asks, and a midprice.
	 *
	 */
	private static class BitFlyerOrderBookResponse {
		@SerializedName("mid_price")
		private BigDecimal midPrice;

		@SerializedName("bids")
		private List<BitFlyerTick> bids = new ArrayList<>();

		@SerializedName("asks")
		private List<BitFlyerTick> asks;

		public BigDecimal getMidPrice() {
			return midPrice;
		}

		public void setMidPrice(BigDecimal midPrice) {
			this.midPrice = midPrice;
		}

		public List<BitFlyerTick> getBids() {
			return bids;
		}

		public void setBids(List<BitFlyerTick> bids) {
			this.bids = bids;
		}

		public List<BitFlyerTick> getAsks() {
			return asks;
		}

		public void setAsks(List<BitFlyerTick> asks) {
			this.asks = asks;
		}
	}

	/**
	 * A tick, just a price and size.
	 *
	 */
	private static class BitFlyerTick {
		@SerializedName("price")
		private BigDecimal price;

		@SerializedName("size")
		private BigDecimal size;

		public BigDecimal getPrice() {
			return price;
		}

		public void setPrice(BigDecimal price) {
			this.price = price;
		}

		public BigDecimal getSize() {
			return size;
		}

		public void setSize(BigDecimal size) {
			this.size = size;
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
