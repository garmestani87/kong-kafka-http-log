local producers = require "kong.plugins.kong-kafka-log.producers"
local basic_serializer = require "kong.plugins.log-serializers.basic"
local body_transformer = require "kong.plugins.response-transformer.body_transformer"
local cjson = require "cjson"
local is_json_body = body_transformer.is_json_body
local cjson_decode = cjson.decode
local cjson_encode = cjson.encode
local producer
local kong = kong

local KongKafkaLogHandler = {}

KongKafkaLogHandler.PRIORITY = 5
KongKafkaLogHandler.VERSION = "1.0.1"

local function parse_body(type, data)
  if type and data and is_json_body(type) then
    return cjson_decode(data)
  end
end

function KongKafkaLogHandler:access(conf)
  if is_json_body(kong.request.get_header("Content-Type")) then
    local ctx = kong.ctx.plugin;
    ctx.request_body = kong.request.get_raw_body();
  end
end

function KongKafkaLogHandler:body_filter(conf)
  if is_json_body(kong.response.get_header("Content-Type")) then
    local ctx = kong.ctx.plugin;
    local chunk, eof = ngx.arg[1], ngx.arg[2];
    if not eof then
      ctx.response_body = (ctx.response_body or "") .. (chunk or "")
    end
  end
end

--- Publishes a message to Kafka.
-- Must run in the context of `ngx.timer.at`.
local function log(premature, conf, message)
  if premature then
    return
  end

  --Temporary for debugging
  --kong.log.err("current Kafka log json format: ", cjson_encode(message))

  if not producer then
    local err
    producer, err = producers.new(conf)
    if not producer then
      kong.log.err("[kong-kafka-log] failed to create a Kafka Producer for a given configuration: ", err)
      return
    end
  end

  local ok, err = producer:send(conf.topic, nil, cjson_encode(message))
  if not ok then
    kong.log.err("[kong-kafka-log] failed to send a message on topic ", conf.topic, ": ", err)
    return
  end
end

function KongKafkaLogHandler:log(conf)

  local ctx = kong.ctx.plugin;
  local message = basic_serializer.serialize(ngx, nil, conf)

  message.request.body = parse_body(kong.request.get_header("Content-Type"), ctx.request_body)
  message.response.body = parse_body(kong.response.get_header("Content-Type"), ctx.response_body)

  local ok, err = ngx.timer.at(0, log, conf, message)
  if not ok then
    kong.log.err("[kong-kafka-log] failed to create timer: ", err)
  end
end

return KongKafkaLogHandler
