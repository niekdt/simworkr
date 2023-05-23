context('Redis')
skip_if_not(
    condition = isTRUE(as.logical(Sys.getenv('WITH_REDIS', unset = 'true'))),
    message = 'Redis disabled'
)

test_that('Connection', {
    connect()
    expect_true(isConnected())
    expect_gt(clientCount(), 0)
    disconnect()
    expect_false(isConnected())
})
