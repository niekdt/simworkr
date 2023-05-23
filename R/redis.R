#' @export
#' @import rredis
#' @import assertthat
#' @import data.table
#' @import magrittr
#' @title Connect to Redis server
#' @inherit rredis::redisConnect
#' @seealso \link{disconnect}, \link{clientCount}, \link{redisConnect}
connect = function(host=getOption('redis.host', 'localhost'),
                   port=getOption('redis.port', 6379),
                   password=getOption('redis.pwd'),
                   timeout=2678399L,
                   nodelay=TRUE) {
    con = redisConnect(host=host, port=port, password=password, timeout=timeout, nodelay=nodelay, returnRef=TRUE)

    if(isConnected()) {
        message(sprintf('  Connected to Redis at %s:%s.', host, port))
        options('redis:num'=TRUE)
    }
}

#' @export
#' @title Assess whether the session has a connection with the Redis server
isConnected = function() {
    tryCatch({redisInfo(); TRUE}, error=function(e) FALSE)
}

#' @export
#' @title Initiate an asynchronous save-to-disk on the server
bgSave = function() {
    redisBgSave()
}

#' @export
#' @title Disconnect from the Redis server
#' @seealso \link{shutdown}, \link{redisClose}
disconnect = function(save=TRUE) {
    redisClose()
    message('= Redis connection closed.')
}

#' @export
#' @title Shuts down the Redis server
shutdown = function() {
    message(': Shutting down Redis server...')
    redisSetPipeline(TRUE)
    redisShutdown()
    redisSetContext(NULL)
    message('= Redis connection closed.')
}

#' @export
#' @title Get the number of clients connected to the Redis server (including non-workers)
clientCount = function() {
    assert_that(isConnected())
    as.integer(redisInfo()$connected_clients)
}

getRedisInfo = function() {
    redisInfo()
}