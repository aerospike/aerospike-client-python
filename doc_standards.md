
	# Docstring Standards for Fluent Client SDK

**Audience**: Engineering (Java and Python client teams)  
**Purpose**: Define documentation requirements for public APIs to ensure quality auto-generated API reference  
**Owner**: DevEd + Engineering Leads  
**Created**: 2026-01-20

---

## Why This Matters

The Fluent Client SDK documentation is auto-generated from your code annotations. **Your docstrings ARE the API reference.** Poor docstrings = poor developer experience = more support tickets.

Per the PRD, a key success metric is:
> "AI coding agents able to successfully complete 80% of coding operations within 3 shots"

LLMs parse docstrings directly. If your docstrings are sparse, AI coding assistants will generate bad code for our customers.

---

## Requirements Summary

Every **public method, class, and constant** must have:

| Requirement | Java | Python |
|-------------|------|--------|
| One-sentence description | ✓ | ✓ |
| Detailed explanation (when needed) | ✓ | ✓ |
| Working code example | ✓ | ✓ |
| All parameters documented | ✓ | ✓ |
| Return value documented | ✓ | ✓ |
| Exceptions/errors documented | ✓ | ✓ |
| Cross-references to related methods | ✓ | ✓ |
| Link to relevant guide (when applicable) | ✓ | ✓ |

**CI will fail if public APIs lack documentation.**

---

## Java (Javadoc) Standards

### Basic Structure

```java
/**
 * [One-sentence description of what this method does.]
 *
 * <p>[Optional: More detailed explanation of behavior, when to use this method,
 * and any important considerations. Use multiple paragraphs if needed.]
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * [Working code example that can be copy-pasted]
 * }</pre>
 *
 * @param paramName [Description of parameter, including valid values]
 * @return [Description of what is returned]
 * @throws ExceptionType [When this exception is thrown]
 * @see #relatedMethod() [Link to related method]
 * @see <a href="https://aerospike.com/docs/...">Guide Name</a>
 */
```

### Complete Example: Insert Method

```java
/**
 * Inserts a new record into the database, failing if the record already exists.
 *
 * <p>Use this method when you need to guarantee the record is new. If the record
 * may already exist and you want to overwrite it, use {@link #upsert(DataSet)} instead.
 *
 * <p>The insert operation is atomic. If the record exists, no partial write occurs.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * // Insert a new customer record
 * DataSet customers = DataSet.of("production", "customers");
 * 
 * session.insertInto(customers.id("cust-12345"))
 *     .bin("name", "Alice Smith")
 *     .bin("email", "alice@example.com")
 *     .bin("created", Instant.now().toEpochMilli())
 *     .execute();
 * }</pre>
 *
 * <p><b>Example with error handling:</b>
 * <pre>{@code
 * try {
 *     session.insertInto(customers.id("cust-12345"))
 *         .bin("name", "Alice Smith")
 *         .execute();
 * } catch (RecordExistsException e) {
 *     // Record already exists - handle duplicate
 *     log.warn("Customer {} already exists", e.getKey());
 * }
 * }</pre>
 *
 * @param dataSet the target dataset containing namespace, set, and record key;
 *                must not be {@code null}
 * @return a fluent {@link InsertBuilder} for configuring bins and executing the insert
 * @throws NullPointerException if {@code dataSet} is {@code null}
 * @throws RecordExistsException if a record with this key already exists
 * @throws AerospikeException if a cluster communication error occurs
 * @see #upsert(DataSet) for insert-or-update behavior
 * @see #update(DataSet) for updating existing records only
 * @see <a href="https://aerospike.com/docs/develop/client/java-fluent/usage/atomic/create">Creating Records Guide</a>
 */
public InsertBuilder insertInto(DataSet dataSet) {
    // implementation
}
```

### Complete Example: Query with DSL

```java
/**
 * Creates a query builder for retrieving records that match filter criteria.
 *
 * <p>Use the DSL expression syntax to define filters using familiar operators:
 * <ul>
 *   <li>{@code ==}, {@code !=} for equality</li>
 *   <li>{@code <}, {@code >}, {@code <=}, {@code >=} for comparison</li>
 *   <li>{@code and}, {@code or}, {@code not} for logical operations</li>
 *   <li>{@code contains}, {@code startsWith} for string operations</li>
 * </ul>
 *
 * <p><b>Example - Simple filter:</b>
 * <pre>{@code
 * DataSet users = DataSet.of("app", "users");
 * 
 * List<Record> activeUsers = session.query(users)
 *     .where("status == 'active'")
 *     .select("name", "email")
 *     .execute()
 *     .toList();
 * }</pre>
 *
 * <p><b>Example - Complex filter:</b>
 * <pre>{@code
 * List<Record> results = session.query(orders)
 *     .where("country == 'US' and total > 100 and status != 'cancelled'")
 *     .select("orderId", "total", "items")
 *     .limit(50)
 *     .execute()
 *     .toList();
 * }</pre>
 *
 * <p><b>Example - Iterating results:</b>
 * <pre>{@code
 * session.query(users)
 *     .where("age >= 21")
 *     .execute()
 *     .forEach(record -> {
 *         String name = record.getString("name");
 *         System.out.println("Found user: " + name);
 *     });
 * }</pre>
 *
 * @param dataSet the dataset (namespace and set) to query; must not be {@code null}
 * @return a fluent {@link QueryBuilder} for adding filters, projections, and executing
 * @throws NullPointerException if {@code dataSet} is {@code null}
 * @see <a href="https://aerospike.com/docs/develop/client/java-fluent/usage/multi/queries">Query Guide</a>
 * @see <a href="https://aerospike.com/docs/develop/client/java-fluent/concepts/dsl">DSL Expression Reference</a>
 */
public QueryBuilder query(DataSet dataSet) {
    // implementation
}
```

### Complete Example: Behavior Configuration

```java
/**
 * A pre-configured set of operational policies optimized for low-latency reads.
 *
 * <p>This behavior configures:
 * <ul>
 *   <li>Socket timeout: 50ms</li>
 *   <li>Total timeout: 100ms</li>
 *   <li>Max retries: 1</li>
 *   <li>Replica policy: Prefer master, fall back to replica</li>
 * </ul>
 *
 * <p>Use this behavior for user-facing read operations where latency is critical
 * and occasional failures are acceptable. For operations that must succeed,
 * use {@link #DURABLE_READ} instead.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * // Create a session with low-latency read behavior
 * Session session = cluster.createSession(Behavior.LOW_LATENCY_READ);
 * 
 * // All reads through this session use the low-latency settings
 * Optional<Record> user = session.get(users.id("user-123")).execute();
 * }</pre>
 *
 * @see #DURABLE_READ for reads that must succeed
 * @see #BALANCED for general-purpose operations
 * @see <a href="https://aerospike.com/docs/develop/client/java-fluent/concepts/behaviors">Behaviors Guide</a>
 */
public static final Behavior LOW_LATENCY_READ = new Behavior.Builder()
    .socketTimeout(50)
    .totalTimeout(100)
    .maxRetries(1)
    .build();
```

### Java Checklist

Before submitting a PR, verify each public API has:

- [ ] First sentence is a complete, standalone description
- [ ] Uses `<p>` tags for paragraph breaks (not blank lines)
- [ ] Code examples wrapped in `<pre>{@code ... }</pre>`
- [ ] Every `@param` describes valid values, not just the type
- [ ] `@return` describes what the value represents
- [ ] `@throws` lists all checked AND unchecked exceptions that callers should handle
- [ ] `@see` links to related methods and guides
- [ ] Examples are complete (include imports if non-obvious)
- [ ] Examples compile and run

---

## Python (Google Style) Standards

### Basic Structure

```python
def method_name(self, param: Type) -> ReturnType:
    """One-sentence description of what this method does.

    Optional: More detailed explanation of behavior, when to use this method,
    and any important considerations. Can span multiple paragraphs.

    Example:
        >>> # Working code example
        >>> result = client.method_name(param)
        >>> print(result)
        expected_output

    Args:
        param: Description of parameter, including valid values.

    Returns:
        Description of what is returned.

    Raises:
        ExceptionType: When this exception is raised.

    See Also:
        :meth:`related_method`: Description of relationship.
        `Guide Name <https://aerospike.com/docs/...>`_
    """
```

### Complete Example: Insert Method

```python
def insert_into(self, dataset: DataSet) -> InsertBuilder:
    """Insert a new record into the database, failing if it already exists.

    Use this method when you need to guarantee the record is new. If the record
    may already exist and you want to overwrite it, use :meth:`upsert` instead.

    The insert operation is atomic. If the record exists, no partial write occurs.

    Example:
        >>> # Insert a new customer record
        >>> customers = DataSet.of("production", "customers")
        >>> 
        >>> session.insert_into(customers.id("cust-12345")) \\
        ...     .bin("name", "Alice Smith") \\
        ...     .bin("email", "alice@example.com") \\
        ...     .bin("created", int(time.time() * 1000)) \\
        ...     .execute()

    Example with error handling:
        >>> try:
        ...     session.insert_into(customers.id("cust-12345")) \\
        ...         .bin("name", "Alice Smith") \\
        ...         .execute()
        ... except RecordExistsError as e:
        ...     # Record already exists - handle duplicate
        ...     print(f"Customer {e.key} already exists")

    Args:
        dataset: The target dataset containing namespace, set, and record key.
            Must not be None.

    Returns:
        A fluent InsertBuilder for configuring bins and executing the insert.

    Raises:
        ValueError: If dataset is None.
        RecordExistsError: If a record with this key already exists.
        AerospikeError: If a cluster communication error occurs.

    See Also:
        :meth:`upsert`: For insert-or-update behavior.
        :meth:`update`: For updating existing records only.
        `Creating Records Guide <https://aerospike.com/docs/develop/client/python-fluent/usage/atomic/create>`_
    """
    # implementation
```

### Complete Example: Query with DSL

```python
def query(self, dataset: DataSet) -> QueryBuilder:
    """Create a query builder for retrieving records that match filter criteria.

    Use the DSL expression syntax to define filters using familiar operators:
    
    - ``==``, ``!=`` for equality
    - ``<``, ``>``, ``<=``, ``>=`` for comparison  
    - ``and``, ``or``, ``not`` for logical operations
    - ``contains``, ``startswith`` for string operations

    Example - Simple filter:
        >>> users = DataSet.of("app", "users")
        >>> 
        >>> active_users = session.query(users) \\
        ...     .where("status == 'active'") \\
        ...     .select("name", "email") \\
        ...     .execute() \\
        ...     .to_list()

    Example - Complex filter:
        >>> results = session.query(orders) \\
        ...     .where("country == 'US' and total > 100 and status != 'cancelled'") \\
        ...     .select("order_id", "total", "items") \\
        ...     .limit(50) \\
        ...     .execute() \\
        ...     .to_list()

    Example - Iterating results:
        >>> for record in session.query(users).where("age >= 21").execute():
        ...     name = record.get_string("name")
        ...     print(f"Found user: {name}")

    Args:
        dataset: The dataset (namespace and set) to query. Must not be None.

    Returns:
        A fluent QueryBuilder for adding filters, projections, and executing.

    Raises:
        ValueError: If dataset is None.

    See Also:
        `Query Guide <https://aerospike.com/docs/develop/client/python-fluent/usage/multi/queries>`_
        `DSL Expression Reference <https://aerospike.com/docs/develop/client/python-fluent/concepts/dsl>`_
    """
    # implementation
```

### Complete Example: Behavior Configuration

```python
class Behavior:
    """Pre-configured operational policies for common use cases."""

    LOW_LATENCY_READ: "Behavior" = None  # Initialized below
    """Pre-configured policies optimized for low-latency reads.

    This behavior configures:
    
    - Socket timeout: 50ms
    - Total timeout: 100ms
    - Max retries: 1
    - Replica policy: Prefer master, fall back to replica

    Use this behavior for user-facing read operations where latency is critical
    and occasional failures are acceptable. For operations that must succeed,
    use :attr:`DURABLE_READ` instead.

    Example:
        >>> # Create a session with low-latency read behavior
        >>> session = cluster.create_session(Behavior.LOW_LATENCY_READ)
        >>> 
        >>> # All reads through this session use the low-latency settings
        >>> user = session.get(users.id("user-123")).execute()

    See Also:
        :attr:`DURABLE_READ`: For reads that must succeed.
        :attr:`BALANCED`: For general-purpose operations.
        `Behaviors Guide <https://aerospike.com/docs/develop/client/python-fluent/concepts/behaviors>`_
    """
```

### Python Checklist

Before submitting a PR, verify each public API has:

- [ ] First line is a complete, standalone description (imperative mood)
- [ ] Blank line after first line if there's more content
- [ ] Examples use doctest format (`>>>`) when possible
- [ ] Args section describes valid values, not just types
- [ ] Returns section describes what the value represents
- [ ] Raises section lists all exceptions callers should handle
- [ ] See Also section links to related methods and guides
- [ ] Examples are complete and runnable
- [ ] Type hints match docstring descriptions

---

## Common Patterns

### Fluent Builder Methods

For methods that return `self` for chaining:

**Java:**
```java
/**
 * Sets the time-to-live for this record in seconds.
 *
 * <p>After the TTL expires, the record is automatically deleted by the server.
 * Use {@code -1} to inherit the namespace default, or {@code 0} to never expire.
 *
 * @param seconds the TTL in seconds; use -1 for namespace default, 0 for never expire
 * @return this builder for method chaining
 * @see <a href="https://aerospike.com/docs/develop/client/java-fluent/usage/atomic/create#ttl">TTL Guide</a>
 */
public InsertBuilder ttl(int seconds) {
    // implementation
    return this;
}
```

**Python:**
```python
def ttl(self, seconds: int) -> "InsertBuilder":
    """Set the time-to-live for this record in seconds.

    After the TTL expires, the record is automatically deleted by the server.
    Use -1 to inherit the namespace default, or 0 to never expire.

    Args:
        seconds: The TTL in seconds. Use -1 for namespace default, 0 for never expire.

    Returns:
        This builder for method chaining.

    See Also:
        `TTL Guide <https://aerospike.com/docs/develop/client/python-fluent/usage/atomic/create#ttl>`_
    """
```

### Error Classes

**Java:**
```java
/**
 * Thrown when attempting to insert a record that already exists.
 *
 * <p>This exception indicates the record key is already present in the database.
 * To overwrite existing records, use {@link Session#upsert(DataSet)} instead.
 *
 * <p><b>Handling example:</b>
 * <pre>{@code
 * try {
 *     session.insertInto(dataset).bin("name", "Alice").execute();
 * } catch (RecordExistsException e) {
 *     Key existingKey = e.getKey();
 *     // Decide: skip, update, or fail
 * }
 * }</pre>
 *
 * @see Session#upsert(DataSet) for insert-or-update behavior
 */
public class RecordExistsException extends AerospikeException {
```

**Python:**
```python
class RecordExistsError(AerospikeError):
    """Raised when attempting to insert a record that already exists.

    This exception indicates the record key is already present in the database.
    To overwrite existing records, use :meth:`Session.upsert` instead.

    Example:
        >>> try:
        ...     session.insert_into(dataset).bin("name", "Alice").execute()
        ... except RecordExistsError as e:
        ...     existing_key = e.key
        ...     # Decide: skip, update, or fail

    Attributes:
        key: The key of the existing record.

    See Also:
        :meth:`Session.upsert`: For insert-or-update behavior.
    """
```

---

## Anti-Patterns to Avoid

### ❌ Don't: Sparse Documentation

```java
// BAD - Missing everything useful
/**
 * Inserts a record.
 */
public InsertBuilder insertInto(DataSet dataSet) {
```

### ❌ Don't: Describe Only the Type

```java
// BAD - "dataSet is a DataSet" tells us nothing
/**
 * @param dataSet the DataSet
 */
```

### ❌ Don't: Skip Exceptions

```java
// BAD - Callers don't know what to catch
/**
 * @return the record
 */
// Throws RecordNotFoundException, TimeoutException, AerospikeException...
```

### ❌ Don't: Use Non-Runnable Examples

```java
// BAD - This won't compile (missing setup)
/**
 * <pre>{@code
 * session.insertInto(ds).bin("x", 1).execute();
 * }</pre>
 */
// Where does 'session' come from? What is 'ds'?
```

### ❌ Don't: Forget Cross-References

```java
// BAD - Developer has to search for alternatives
/**
 * Inserts a record, failing if it exists.
 */
// No mention of upsert() as alternative
```

---

## Validation

### CI Checks (To Be Implemented)

1. **Javadoc warnings as errors**: `-Xdoclint:all` flag
2. **Python docstring coverage**: `interrogate` or `pydocstyle`
3. **Example extraction and testing**: Custom script to extract and run code blocks

### Code Review Checklist

Reviewers should verify:

- [ ] Every new public API has complete documentation
- [ ] Examples are realistic (not just `foo`, `bar`, `x`)
- [ ] Cross-references point to actual methods
- [ ] Guide links are valid URLs
- [ ] Documentation matches actual behavior

---

## Questions?

Contact DevEd (JD Gebicki) or the Fluent Client PM (Brian Porter) for clarification on documentation requirements.