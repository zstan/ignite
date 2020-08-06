/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "impl/response_status.h"
#include "impl/message.h"
#include "impl/utility.h"
#include "impl/transactions/transactions_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::thin;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                #define DEFAULT_CONCURRENCY TransactionConcurrency::PESSIMISTIC
                #define DEFAULT_ISOLATION TransactionIsolation::READ_COMMITTED
                #define DEFAULT_TIMEOUT 0
                #define DEFAULT_TX_SIZE 0

                TransactionImpl::TL_SP_TransactionsImpl TransactionImpl::threadTx;

                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) :
                    router(router)
                {
                    // No-op.
                }

                TransactionsImpl::~TransactionsImpl()
                {
                    // No-op.
                }

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SyncMessage(const ReqT& req, RspT& rsp)
                {
                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                SharedPointer<TransactionImpl> TransactionsImpl::TxStart()
                {
                    //IgniteError err = IgniteError(); !!! ???

                    SharedPointer<TransactionImpl> tx = TransactionImpl::Create(this,
                        DEFAULT_CONCURRENCY, DEFAULT_ISOLATION, DEFAULT_TIMEOUT, DEFAULT_TX_SIZE);

                    return tx;
                }

                TransactionImpl::SP_TransactionImpl TransactionImpl::Create(
                    SP_TransactionsImpl txs,
                    TransactionConcurrency::Type concurrency,
                    TransactionIsolation::Type isolation,
                    int64_t timeout,
                    int32_t txSize)
                {
                    TxStartRequest<RequestType::OP_TX_START> req(concurrency, isolation, timeout, txSize);

                    Int32Response rsp;

                    txs.Get()->SyncMessage(req, rsp);

                    int32_t txId = rsp.GetValue();

                    SP_TransactionImpl tx = SP_TransactionImpl(new TransactionImpl(txs, txId, concurrency, isolation, timeout, txSize));

                    threadTx.Set(tx);

                    return tx;
                }

                TransactionImpl::SP_TransactionImpl TransactionImpl::GetCurrent()
                {
                    SP_TransactionImpl tx = threadTx.Get();
                    TransactionImpl* ptr = tx.Get();

                    if (ptr && ptr->IsClosed())
                    {
                        tx = SP_TransactionImpl();

                        threadTx.Remove();
                    }

                    return tx;
                }

                bool TransactionImpl::IsClosed() const
                {
                    return closed;
                }

                SharedPointer<TransactionImpl> TransactionsImpl::GetCurrent()
                {
                    return TransactionImpl::GetCurrent();
                }

                int32_t TransactionsImpl::TxCommit(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, true);

                    Response rsp;

                    SyncMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxRollback(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, false);

                    Response rsp;

                    SyncMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxClose(int32_t txId)
                {
                    return TxRollback(txId);
                }

                void TransactionImpl::Commit()
                {
                    common::concurrent::CsLockGuard guard(accessLock);

                    int32_t rsp = txs.Get()->TxCommit(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }

                void TransactionImpl::Rollback()
                {
                    common::concurrent::CsLockGuard guard(accessLock);

                    int32_t rsp = txs.Get()->TxRollback(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }

                void TransactionImpl::Close()
                {
                    common::concurrent::CsLockGuard guard(accessLock);

                    if (IsClosed())
                    {
                        return;
                    }

                    int32_t rsp = txs.Get()->TxClose(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }
            }
        }
    }
}
