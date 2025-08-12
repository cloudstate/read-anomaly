package org.cloudstate;

import static java.lang.Integer.parseInt;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.IntStream.range;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.sso.auth.ExpiredTokenException;

public final class ReadAnomaly {

	private static final String PROFILE = "<your-profile-name>";
	private static final Region REGION = Region.EU_WEST_1;

	private static final String TABLE = "test_table";
	private static final String ID = "id";
	private static final String VERSION = "version";

	private static final int CHILDS = 30;
	private static final boolean CONSISTENT_READ = true;

	public static void main(final String[] args) throws InterruptedException {
		try (final var db = connectToDatabase()) {
			setUp(db);
			run(db);

			System.out.println("No read anomaly detected!");
		} catch (final ExpiredTokenException e) {
			System.out.println("Did you run 'aws sso login'?");
		} catch (final IllegalStateException e) {
			System.out.println(e.getMessage());
		}
	}

	static void setUp(final DynamoDbClient db) {
		System.out.println("Setting up the database with 1 parent and %d childs...".formatted(CHILDS));

		// Store the first parent item
		store(db, Item.create("parent"));

		// Store the first version of each child item
		range(0, CHILDS).forEach(n -> store(db, Item.create("child-" + n)));

		System.out.println("Database setup completed, verifying initial state...");

		verify(db, "parent", 0);
		range(0, CHILDS).forEach(n -> verify(db, "child-" + n, 0));
	}

	static void run(final DynamoDbClient db) throws InterruptedException {
		System.out.println("Running the test...");

		try (final var pool = newFixedThreadPool(CHILDS)) {
			// Using a CountDownLatch to synchronize the start of all child tasks
			final var start = new CountDownLatch(1);

			// Using a CountDownLatch to wait for all child tasks to complete
			final var end = new CountDownLatch(CHILDS);

			// Submitting tasks to add child items concurrently
			range(0, CHILDS).forEach(n -> pool.submit(() -> addChild(db, "child-" + n, start, end)));

			// Start all tasks at the same time
			start.countDown();

			// Wait for all tasks to complete
			end.await();

			pool.shutdown();
			pool.awaitTermination(1, MINUTES);
		}

		System.out.println("Test run completed, verifying results...");

		// The final state of the parent item should have CHILDS + 1 versions
		verify(db, "parent", CHILDS + 1);

		// Each child item has exactly 2 versions (the initial and the updated)
		range(0, CHILDS).forEach(n -> verify(db, "child-" + n, 2));
	}

	static void addChild(final DynamoDbClient db, final String childId, final CountDownLatch start,
			final CountDownLatch end) {
		try {
			// Wait for all tasks to start at the same time
			start.await();

			// Read the most current version of the child item,
			// and update it with the "parent" action
			final Item child = read(db, childId).update("parent");

			Item parent;

			while (true) {
				// Read the most current version of the parent item,
				// and update it with the childId as the action
				parent = read(db, "parent").update(childId);

				if (store(db, parent, child)) {
					break; // If the store was successful, exit the loop
				}

				// The store failed, and we need to retry
			}
		} catch (final IllegalStateException e) {
			// IF the read anomaly occurs...
			System.out.println(e.getMessage() + " - abort task");
		} catch (final InterruptedException e) {
			currentThread().interrupt();
		} catch (final Exception e) {
			// Catch any other exceptions that may occur
			e.printStackTrace();
		} finally {
			// Signal that this task has completed
			end.countDown();
		}
	}

	static boolean store(final DynamoDbClient db, final Item... items) {
		// Create a transaction request with the items to store
		final var txItems = stream(items)
				.map(item -> TransactWriteItem.builder()
						.put(item.toPut())
						.build())
				.toList();

		try {
			db.transactWriteItems(request -> request.transactItems(txItems));

			// If the transaction was successful, return true
			return true;
		} catch (final TransactionCanceledException e) {
			// This exception only occurs for the parent item, the first in the list.
			final var code = e.cancellationReasons().getFirst().code();
			if ("ConditionalCheckFailed".equals(code) || "TransactionConflict".equals(code)) {
				// The expected reasons are condition check failure or transaction conflict
				// Handle this exception by returning false, and retrying the operation
				return false;
			}

			// If the exception is not one of the expected ones, rethrow it
			throw e;
		}
	}

	static Item read(final DynamoDbClient db, final String id) {
		// Query for the most current version of the item with the given id
		final var request = QueryRequest.builder()
				.tableName(TABLE)
				.keyConditionExpression("%s = :id".formatted(ID))
				.expressionAttributeValues(Map.of(":id", fromS(id)))
				.scanIndexForward(false)
				.limit(1)
				.consistentRead(CONSISTENT_READ)
				.build();

		final var items = db.query(request).items();
		if (items.isEmpty()) {
			// This should never happen!!
			throw new IllegalStateException("Read anomaly for id: " + id);
		}

		return Item.of(items.getFirst());
	}

	static void verify(final DynamoDbClient db, final String id, final int versions) {
		range(0, versions).forEach(n -> checkExist(db, id, n));
	}

	static void checkExist(final DynamoDbClient db, final String id, final int version) {
		final var request = GetItemRequest.builder()
				.tableName(TABLE)
				.key(Map.of(ID, fromS(id), VERSION, fromN(Integer.toString(version))))
				.build();

		if (!db.getItem(request).hasItem()) {
			throw new IllegalStateException("Expected id: %s, version: %d to exist".formatted(id, version));
		}
	}

	static DynamoDbClient connectToDatabase() {
		final var provider = ProfileCredentialsProvider.builder()
				.profileName(PROFILE)
				.build();

		return DynamoDbClient.builder()
				.credentialsProvider(provider)
				.region(REGION)
				.build();
	}

	static record Item(String id, int version, String action) {

		private static final String ACTION = "action";
		private static final String NE = "attribute_not_exists(%s) and attribute_not_exists(%s)"
				.formatted(ID, VERSION);

		public static Item create(final String id) {
			return new Item(id, 0, "create");
		}

		public Item update(final String action) {
			return new Item(id, version + 1, action);
		}

		public Put toPut() {
			final var item = Map.of(ID, fromS(id),
					VERSION, fromN(Integer.toString(version)),
					ACTION, fromS(action));

			return Put.builder()
					.tableName(TABLE)
					.conditionExpression(NE)
					.item(item)
					.build();
		}

		public static Item of(final Map<String, AttributeValue> item) {
			final var id = item.get(ID).s();
			final var version = parseInt(item.get(VERSION).n());
			final var action = item.get(ACTION).s();

			return new Item(id, version, action);
		}

	}

}
