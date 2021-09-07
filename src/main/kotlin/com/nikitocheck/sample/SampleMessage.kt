package com.nikitocheck.sample

import reactor.kafka.receiver.ReceiverOffset

data class SampleMessage(val key: Int, val value: String, val receiverOffset: ReceiverOffset)