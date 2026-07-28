// Package client provides role-specific producer and consumer clients for a
// pubsub.Bus.
//
// A producer or consumer client is bound to one exact topic name and event
// type. Clients share the bus they are constructed with; the application that
// owns that bus remains responsible for calling pubsub.Shutdown.
package client
