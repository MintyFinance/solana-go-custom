// Copyright 2021 github.com/gagliardetto
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ws

import (
	"github.com/MintyFinance/solana-go-custom"
	"github.com/MintyFinance/solana-go-custom/rpc"
)

type TransactionResult struct {
	Transaction *rpc.GetTransactionResult `json:"transaction"`
	Signature   string                    `json:"signature"`
}

type TransactionSubscribeOpts struct {
	AccountInclude                 []solana.PublicKey
	AccountExclude                 []solana.PublicKey
	AccountRequired                []solana.PublicKey
	Commitment                     rpc.CommitmentType
	MaxSupportedTransactionVersion uint
	TransactionDetails             rpc.TransactionDetailsType
	IncludeFailed                  bool
	Encoding                       solana.EncodingType
}

// SignatureSubscribe subscribes to a transaction signature to receive
// notification when the transaction is confirmed On signatureNotification,
// the subscription is automatically cancelled
func (cl *Client) TransactionSubscribe(
	opts *TransactionSubscribeOpts,
) (*TransactionSubscription, error) {
	filters := rpc.M{
		"failed": opts.IncludeFailed,
	}

	if len(opts.AccountInclude) > 0 {
		filters["accountInclude"] = opts.AccountInclude
	}

	if len(opts.AccountExclude) > 0 {
		filters["accountExclude"] = opts.AccountExclude
	}

	if len(opts.AccountRequired) > 0 {
		filters["accountRequired"] = opts.AccountRequired
	}

	params := []interface{}{filters}
	conf := map[string]interface{}{}
	conf["commitment"] = opts.Commitment
	conf["maxSupportedTransactionVersion"] = opts.MaxSupportedTransactionVersion
	conf["transaction_details"] = opts.TransactionDetails
	conf["encoding"] = solana.EncodingBase64

	genSub, err := cl.subscribe(
		params,
		conf,
		"transactionSubscribe",
		"transactionUnsubscribe",
		func(msg []byte) (interface{}, error) {
			var res TransactionResult
			err := decodeResponseFromMessage(msg, &res)
			return &res, err
		},
	)
	if err != nil {
		return nil, err
	}
	return &TransactionSubscription{
		sub: genSub,
	}, nil
}

type TransactionSubscription struct {
	sub *Subscription
}

func (sw *TransactionSubscription) Recv() (*TransactionResult, error) {
	select {
	case d := <-sw.sub.stream:
		return d.(*TransactionResult), nil
	case err := <-sw.sub.err:
		return nil, err
	}
}

func (sw *TransactionSubscription) Err() <-chan error {
	return sw.sub.err
}

func (sw *TransactionSubscription) Response() <-chan *TransactionResult {
	typedChan := make(chan *TransactionResult, 100) // can be buffered if needed
	go func(ch chan *TransactionResult) {
		defer close(ch)
		for d := range sw.sub.stream {
			if transactionResult, ok := d.(*TransactionResult); ok {
				ch <- transactionResult
			}
		}
	}(typedChan)
	return typedChan
}

func (sw *TransactionSubscription) Unsubscribe() {
	sw.sub.Unsubscribe()
}
