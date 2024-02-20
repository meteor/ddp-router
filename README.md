# DDP Router

## Quick start

1. Add the following code to your Meteor app:
    ```ts
    import { Meteor } from 'meteor/meteor';
    import { NpmModuleMongodb } from 'meteor/npm-mongo';

    const { publish } = Meteor;
    Meteor.publish = function publishWithDDPRouter(name, fn) {
      Meteor.methods({
        [`__subscription__${name}`]() {
          const context = { ...this, ready() {}, unblock() {} };
          const maybeCursorOrCursors = fn.apply(context, arguments);
          const cursors =
            Array.isArray(maybeCursorOrCursors)
              ? maybeCursorOrCursors
              : maybeCursorOrCursors
              ? [maybeCursorOrCursors]
              : [];
          const cursorDescriptions = cursors.map(cursor => cursor._cursorDescription);
          // Use BSON's EJSON instead of Meteor's one and return a string to make
          // sure the latter won't interfere.
          return NpmModuleMongodb.BSON.EJSON.stringify(cursorDescriptions);
        },
      });

      return publish.apply(this, arguments);
    };
    ```
    * Note that it has to run **before** any publication is registered.
    * Make sure to exclude all low-level publications (i.e., ones that use `this.added` and similar APIs directly) and other publish-like packages (e.g., publish composite).
1. Start Meteor with two additional flags:
    * `DISABLE_SOCKJS=true` to disable the SockJS communication format and additional handshakes.
    * `DDP_DEFAULT_CONNECTION_URL=http://127.0.0.1:4000` to make the browser connect through the DDP Router.
1. Start DDP Router:
    * Provide required configuration:
        * `METEOR_URL`, e.g., `ws://127.0.0.1:3000/websocket`.
        * `MONGO_URL`, e.g., `mongodb://127.0.0.1:3001/?directConnection=true`.
        * `ROUTER_URL`, e.g., `127.0.0.1:4000`.
    * `cargo run` starts it in a debug mode (add `--release` for release mode).
    * Alternatively, build it with `cargo build` and run manually.
1. Use the Meteor app as normal.

## What it does

When the client connects to the DDP Router, the DDP Router connects to the Meteor server and forwards all DDP messages both ways. That's the base the following flows rely on.

### Subscriptions

1. Client starts a subscription (`sub` DDP message).
1. DDP Router intercepts it and tries to execute `__subscription__${subscriptionName}` method using the same arguments.
1. If it fails, DDP Router forwards the `sub` message as follows.
    * This is needed for incompatible publications, e.g., ones that were not registered in the patched `Meteor.publish`, or ones that threw an error (e.g., they used low-level publication APIs).
1. If it succeeds, DDP Router registers the subscription.
1. DDP Router periodically reruns the subscriptions, updates the Mergebox, and sends corresponding DDP messages.
    * It also sends the `ready` DDP message after the first subscription run.
1. When the client stops a subscription (`unsub` DDP message), DDP Router checks if it manages it and if so, unregisters the subscription and stops it (`nosub` DDP message).

### Other live-data messages

1. Server may send all sorts of live-data messages to the client (e.g., `added`) for a couple of reasons: global publications, not registered and low-level ones, and just because it wants to (some packages do that).
1. DDP Router intercepts those and applies them to Mergebox to make sure the client receives only the relevant messages.
    * It's especially important when the same collection is published both from the DDP Router and the Meteor server.

## Limitations and known issues

* **No resumption handling.** When an error occurs either on the client or server connection, both connections are closed.
* **Different `$regex` dialect.** PCRE2 is not feasible in Rust, so we use [`regex`](https://crates.io/crates/regex). It's mostly compatible, though.
* **A limited support for real-time database updates.** If DDP Router can fully understand the query (including its projection, sorting, etc.) then it'll runt a Change Stream. If not, it'll fall back to pooling instead.
    * Missing query operators: `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$elemMatch`, and `$where` (not possible).
    * No projection operators.
    * No sorting on parallel dotted paths (e.g., `{'a.x': 1, 'a.y': 1}`).
    * No `skip`. We _could_ support it, but we'll need to store `limit + skip` documents in memory anyway. Maybe make a configurable limit for it?
* **Collections with `ObjectId` in the `_id` field.** It looks like Meteor does not use `EJSON` for serializing the `_id` field, but DDP Router does. Instead of patching the DDP Router, patch the Meteor app using the following code:
    ```ts
    import { MongoID } from 'meteor/mongo-id';

    const idParse = MongoID.idParse;
    MongoID.idParse = id => (id?.$type === 'oid' ? idParse(id.$value) : idParse(id));
    ```
