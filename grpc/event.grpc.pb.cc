// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: event.proto

#include "event.pb.h"
#include "event.grpc.pb.h"

#include <functional>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/message_allocator.h>
#include <grpcpp/support/method_handler.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/server_context.h>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/support/sync_stream.h>
namespace events {

static const char* EventService_method_names[] = {
  "/events.EventService/SendEvent",
};

std::unique_ptr< EventService::Stub> EventService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< EventService::Stub> stub(new EventService::Stub(channel, options));
  return stub;
}

EventService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_SendEvent_(EventService_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status EventService::Stub::SendEvent(::grpc::ClientContext* context, const ::events::Event& request, ::events::Ack* response) {
  return ::grpc::internal::BlockingUnaryCall< ::events::Event, ::events::Ack, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_SendEvent_, context, request, response);
}

void EventService::Stub::async::SendEvent(::grpc::ClientContext* context, const ::events::Event* request, ::events::Ack* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::events::Event, ::events::Ack, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendEvent_, context, request, response, std::move(f));
}

void EventService::Stub::async::SendEvent(::grpc::ClientContext* context, const ::events::Event* request, ::events::Ack* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_SendEvent_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::events::Ack>* EventService::Stub::PrepareAsyncSendEventRaw(::grpc::ClientContext* context, const ::events::Event& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::events::Ack, ::events::Event, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_SendEvent_, context, request);
}

::grpc::ClientAsyncResponseReader< ::events::Ack>* EventService::Stub::AsyncSendEventRaw(::grpc::ClientContext* context, const ::events::Event& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncSendEventRaw(context, request, cq);
  result->StartCall();
  return result;
}

EventService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      EventService_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< EventService::Service, ::events::Event, ::events::Ack, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](EventService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::events::Event* req,
             ::events::Ack* resp) {
               return service->SendEvent(ctx, req, resp);
             }, this)));
}

EventService::Service::~Service() {
}

::grpc::Status EventService::Service::SendEvent(::grpc::ServerContext* context, const ::events::Event* request, ::events::Ack* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace events

