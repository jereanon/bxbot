package com.gazbert.bxbot.exchanges;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.gazbert.bxbot.exchange.api.AuthenticationConfig;
import com.gazbert.bxbot.exchange.api.ExchangeConfig;
import com.gazbert.bxbot.exchange.api.NetworkConfig;
import com.gazbert.bxbot.exchange.api.OptionalConfig;
import com.gazbert.bxbot.trading.api.MarketOrderBook;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.crypto.*"})
@PrepareForTest(BitFlyerExchangeAdapter.class)
public class TestBitFlyerExchangeAdapter {

	// Canned JSON responses from exchange - expected to reside on filesystem relative to project root

	// public
	private static final String BOARD_JSON_RESPONSE = "./src/test/exchange-data/bitflyer/board.json";
	private static final String TICKER_JSON_RESPONSE = "./src/test/exchange-data/bitflyer/ticker.json";

	// protected TODOs
	private static final String CHILD_ORDERS_JSON_RESPONSE = "./src/test/exchange-data/bitflyer/childOrders.json";
	private static final String SEND_CHILD_ORDERS_JSON_RESPONSE = "./src/test/exchange-data/bitflyer/sendOrder.json";
	private static final String BALANCE_JSON_RESPONSE = "./src/test/exchange-data/bitflyer/balance.json";

	// Exchange Adapter config for the tests
	private static final String PASSPHRASE = "lePassPhrase";
	private static final String KEY = "key123";
	private static final String SECRET = "notGonnaTellYa";
	private static final List<Integer> nonFatalNetworkErrorCodes = Arrays.asList(502, 503, 504);
	private static final List<String> nonFatalNetworkErrorMessages = Arrays.asList(
			"Connection refused", "Connection reset", "Remote host closed connection during handshake");

	private ExchangeConfig exchangeConfig;
	private AuthenticationConfig authenticationConfig;
	private NetworkConfig networkConfig;
	private OptionalConfig optionalConfig;

	private static final String MARKET_ID = "BTC-USD";

	@Before
	public void setup() throws Exception {
		authenticationConfig = PowerMock.createMock(AuthenticationConfig.class);
		expect(authenticationConfig.getItem("passphrase")).andReturn(PASSPHRASE);
		expect(authenticationConfig.getItem("key")).andReturn(KEY);
		expect(authenticationConfig.getItem("secret")).andReturn(SECRET);

		networkConfig = PowerMock.createMock(NetworkConfig.class);
		expect(networkConfig.getConnectionTimeout()).andReturn(30);
		expect(networkConfig.getNonFatalErrorCodes()).andReturn(nonFatalNetworkErrorCodes);
		expect(networkConfig.getNonFatalErrorMessages()).andReturn(nonFatalNetworkErrorMessages);

		optionalConfig = PowerMock.createMock(OptionalConfig.class);
		expect(optionalConfig.getItem("buy-fee")).andReturn("0.25");
		expect(optionalConfig.getItem("sell-fee")).andReturn("0.25");

		exchangeConfig = PowerMock.createMock(ExchangeConfig.class);
		expect(exchangeConfig.getAuthenticationConfig()).andReturn(authenticationConfig);
		expect(exchangeConfig.getNetworkConfig()).andReturn(networkConfig);
		expect(exchangeConfig.getOptionalConfig()).andReturn(optionalConfig);
	}

	@Test
	public void testGetMarketOrders() throws Exception {

		final byte[] encoded = Files.readAllBytes(Paths.get(BOARD_JSON_RESPONSE));
		final AbstractExchangeAdapter.ExchangeHttpResponse exchangeHttpResponse =
				new AbstractExchangeAdapter.ExchangeHttpResponse(
						200, "OK", new String(encoded, StandardCharsets.UTF_8));

		// Mock out param map so we can assert the contents passed to the transport layer are what we expect.
		final Map<String, String> requestParamMap = PowerMock.createMock(Map.class);
		expect(requestParamMap.put("product_code", "BTC_USD")).andStubReturn(null);

		// Partial mock so we do not send stuff down the wire
		final BitFlyerExchangeAdapter exchangeAdapter = PowerMock.createPartialMockAndInvokeDefaultConstructor(
				BitFlyerExchangeAdapter.class);

		/*
		PowerMock.expectPrivate(exchangeAdapter, MOCKED_GET_REQUEST_PARAM_MAP_METHOD).andReturn(requestParamMap);
		PowerMock.expectPrivate(exchangeAdapter, MOCKED_SEND_PUBLIC_REQUEST_TO_EXCHANGE_METHOD, eq(BOOK),
				eq(requestParamMap)).andReturn(exchangeResponse);

		PowerMock.replayAll(); */
		exchangeAdapter.init(exchangeConfig);

		final MarketOrderBook marketOrderBook = exchangeAdapter.getMarketOrders(MARKET_ID);
		assertNotNull(marketOrderBook);
	}
}