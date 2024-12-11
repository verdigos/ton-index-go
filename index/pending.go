package index

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/kdimentionaltree/ton-index-go/index/emulated"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"log"
	"strconv"
)

type EmulatedTracesContext struct {
	emulatedTransactionsRaw   map[string]map[string][]interface{}
	emulatedTransactions      map[string][]*emulated.TransactionRow
	emulatedMessageContents   map[string]*emulated.MessageContentRow
	emulatedMessageInitStates map[string]*emulated.MessageContentRow
	emulatedMessages          map[string][]*emulated.MessageRow
	traceIds                  []string
	emulatedOnly              bool
}

func NewEmptyContext(emulated_only bool) *EmulatedTracesContext {
	return &EmulatedTracesContext{
		emulatedTransactionsRaw:   make(map[string]map[string][]interface{}),
		emulatedTransactions:      make(map[string][]*emulated.TransactionRow),
		emulatedMessageContents:   make(map[string]*emulated.MessageContentRow),
		emulatedMessageInitStates: make(map[string]*emulated.MessageContentRow),
		emulatedMessages:          make(map[string][]*emulated.MessageRow),
		traceIds:                  make([]string, 0),
		emulatedOnly:              emulated_only,
	}
}

func (c *EmulatedTracesContext) SetEmulatedOnly(emulatedOnly bool) {
	c.emulatedOnly = emulatedOnly
}
func (c *EmulatedTracesContext) IsEmulatedOnly() bool {
	return c.emulatedOnly
}
func (c *EmulatedTracesContext) IsEmptyContext() bool {
	return len(c.emulatedTransactions) == 0
}

//	func (c *EmulatedTracesContext) ApplyTransactionFilter(filter *index.TransactionRequest) {
//		predicate := func(tx *TransactionRow) bool {
//			return slices.Contains(filter.Account, index.AccountAddress(tx.Account)) &&
//				!(slices.Contains(filter.ExcludeAccount, index.AccountAddress(tx.Account)))
//		}
//		for trace_id, txs := range c.emulatedTransactions {
//			filtered_txs := make([]*TransactionRow, 0)
//			for _, tx := range txs {
//				if predicate(tx) {
//					filtered_txs = append(filtered_txs, tx)
//				}
//			}
//			if len(filtered_txs) > 0 {
//				c.emulatedTransactions[trace_id] = filtered_txs
//			} else {
//				delete(c.emulatedTransactions, trace_id)
//				delete(c.emulatedMessages, trace_id)
//			}
//		}
//	}
func (c *EmulatedTracesContext) RemoveTraces(trace_ids []string) {
	for _, trace_id := range trace_ids {
		delete(c.emulatedTransactions, trace_id)
		delete(c.emulatedMessages, trace_id)
	}
}
func (c *EmulatedTracesContext) GetTransactions() []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, txs := range c.emulatedTransactions {
		for _, tx := range txs {
			rows = append(rows, emulated.NewRow(tx))
		}
	}
	return rows
}

func (c *EmulatedTracesContext) GetMessages(transaction_hashes []string) []pgx.Row {
	rows := make([]pgx.Row, 0)
	for _, tx_hash := range transaction_hashes {
		for _, msg := range c.emulatedMessages[tx_hash] {
			var init_state *emulated.MessageContentRow
			var message_content *emulated.MessageContentRow
			if msg.InitStateHash != nil {
				if state, ok := c.emulatedMessageInitStates[*msg.InitStateHash]; ok {
					init_state = state
				}
			}
			if content, ok := c.emulatedMessageContents[msg.MsgHash]; ok {
				message_content = content
			}
			rows = append(rows, emulated.NewRow(msg, message_content, init_state))
		}
	}
	return rows
}

func (c *EmulatedTracesContext) FillFromRawData(rawData map[string]map[string][]interface{}) error {
	for k, v := range rawData {
		c.emulatedTransactionsRaw[k] = v
	}
	for trace_id := range rawData {
		c.traceIds = append(c.traceIds, trace_id)
		err := c.fillTrace(trace_id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *EmulatedTracesContext) fillTrace(trace_id string) error {
	tx_key_queue := make([]string, 0)
	c.emulatedTransactions[trace_id] = make([]*emulated.TransactionRow, 0)
	tx_key_queue = append(tx_key_queue, trace_id)
	for len(tx_key_queue) > 0 {
		key := tx_key_queue[0]
		tx_key_queue = tx_key_queue[1:]
		tx_slice := c.emulatedTransactionsRaw[trace_id][key]
		next_keys, err := c.fillTransaction(trace_id, tx_slice)
		if err != nil {
			return err
		}
		tx_key_queue = append(tx_key_queue, next_keys...)
	}
	return nil
}

func (c *EmulatedTracesContext) fillTransaction(trace_id string, tx_slice []interface{}) ([]string, error) {
	slice := tx_slice[0].([]interface{})
	emulated_tx := tx_slice[1].(bool)
	should_save := !c.emulatedOnly || emulated_tx

	out_message_hashes := make([]string, 0)
	transaction := &emulated.TransactionRow{}
	transaction.Emulated = emulated_tx
	transaction.TraceID = &trace_id
	transaction.Hash = slice[0].(string)
	transaction.Account = slice[1].(string)
	transaction.Lt = *readOptionalNumeric[int64](slice[2])
	transaction.PrevTransHash = readOptional[string](slice[3])
	transaction.PrevTransLt = readOptionalNumeric[int64](slice[4])
	transaction.Now = readOptionalNumeric[int32](slice[5])
	origStatus, err := toAccountStatus(*(readOptionalNumeric[int](slice[6])))
	if err != nil {
		return nil, err
	}
	transaction.OrigStatus = &origStatus
	endStatus, err := toAccountStatus(*(readOptionalNumeric[int](slice[7])))
	if err != nil {
		return nil, err
	}
	transaction.EndStatus = &endStatus
	transaction.TotalFees = readOptionalNumeric[int64](slice[10])
	err = fillTransactionDescr(transaction, slice[13].([]interface{}))
	if err != nil {
		return nil, err
	}
	if slice[8] != nil {
		in_message_slice := slice[8].([]interface{})
		msg, content, init_state, err := AssembleFromMessageSlice(in_message_slice)
		if err != nil {
			return nil, err
		}
		msg.Direction = "in"
		msg.TxHash = transaction.Hash
		msg.TraceID = &trace_id
		if should_save {
			c.emulatedMessages[trace_id] = append(c.emulatedMessages[trace_id], msg)
			c.emulatedMessageContents[msg.MsgHash] = content
			if init_state != nil {
				c.emulatedMessageInitStates[init_state.Hash] = init_state
			}
		}

	}
	if slice[9] != nil {
		out_messages := slice[9].([]interface{})
		for _, out_message_slice := range out_messages {
			msg, content, init_state, err := AssembleFromMessageSlice(out_message_slice.([]interface{}))
			if err != nil {
				return nil, err
			}
			msg.Direction = "out"
			msg.TxHash = slice[0].(string)
			msg.TraceID = &trace_id
			out_message_hashes = append(out_message_hashes, msg.MsgHash)
			if should_save {
				c.emulatedMessages[trace_id] = append(c.emulatedMessages[trace_id], msg)
				c.emulatedMessageContents[msg.MsgHash] = content
				if init_state != nil {
					c.emulatedMessageInitStates[init_state.Hash] = init_state
				}
			}
		}
	}
	if should_save {
		c.emulatedTransactions[trace_id] = append(c.emulatedTransactions[trace_id], transaction)
	}
	return out_message_hashes, nil
}

func fillTransactionDescr(transaction *emulated.TransactionRow, slice []interface{}) error {
	ord_val := "ord"
	transaction.Descr = &ord_val
	transaction.CreditFirst = readOptional[bool](slice[0])
	if slice[1] != nil {
		storage_phase_slice := slice[1].([]interface{})
		transaction.StorageFeesCollected = readOptionalNumeric[int64](storage_phase_slice[0])
		transaction.StorageFeesDue = readOptionalNumeric[int64](storage_phase_slice[1])
		account_status_change_int := readOptionalNumeric[int8](storage_phase_slice[2])
		if account_status_change_int != nil {
			account_status_change, err := readAccountStatusChange(*account_status_change_int)
			if err != nil {
				return err
			}
			transaction.StorageStatusChange = &account_status_change
		}
	}

	if slice[2] != nil {
		credit_phase_slice := slice[2].([]interface{})
		transaction.CreditDueFeesCollected = readOptionalNumeric[int64](credit_phase_slice[0])
		transaction.Credit = readOptionalNumeric[int64](credit_phase_slice[1])
	}

	if slice[3] != nil {
		fillComputePhase(transaction, slice[3].([]interface{}))
	}

	if slice[4] != nil {
		action_phase_slice := slice[4].([]interface{})
		transaction.ActionSuccess = readOptional[bool](action_phase_slice[0])
		transaction.ActionValid = readOptional[bool](action_phase_slice[1])
		transaction.ActionNoFunds = readOptional[bool](action_phase_slice[2])
		transaction.ActionTotalFwdFees = readOptionalNumeric[int64](action_phase_slice[4])
		transaction.ActionTotalActionFees = readOptionalNumeric[int64](action_phase_slice[5])
		transaction.ActionResultCode = readOptionalNumeric[int32](action_phase_slice[6])
		transaction.ActionResultArg = readOptionalNumeric[int32](action_phase_slice[7])
		transaction.ActionTotActions = readOptionalNumeric[int32](action_phase_slice[8])
		transaction.ActionSpecActions = readOptionalNumeric[int32](action_phase_slice[9])
		transaction.ActionSkippedActions = readOptionalNumeric[int32](action_phase_slice[10])
		transaction.ActionMsgsCreated = readOptionalNumeric[int32](action_phase_slice[11])
		transaction.ActionActionListHash = readOptionalString[string](action_phase_slice[12])

		account_status_change_int := readOptionalNumeric[int8](action_phase_slice[2])
		if account_status_change_int != nil {
			account_status_change, err := readAccountStatusChange(*account_status_change_int)
			if err != nil {
				return err
			}
			transaction.ActionStatusChange = &account_status_change
		}
		if action_phase_slice[13] != nil {
			msg_size_slice := action_phase_slice[13].([]interface{})
			transaction.ActionTotMsgSizeCells, transaction.ActionTotMsgSizeBits = readMsgSize(msg_size_slice)
		}
	}

	transaction.Aborted = readOptional[bool](slice[5])
	if slice[6] != nil {
		fillBouncePhase(transaction, slice[6].([]interface{}))
	}
	transaction.Destroyed = readOptional[bool](slice[7])
	return nil
}

func fillComputePhase(transaction *emulated.TransactionRow, slice []interface{}) {
	compute_phase_type := slice[0].(int64)
	compute_phase_data_slice := slice[1].([]interface{})
	if compute_phase_type == 0 {
		transaction.ComputeSkipped = new(bool)
		*transaction.ComputeSkipped = true
		reason_int := *readOptionalNumeric[int32](compute_phase_data_slice[0])
		var reason string
		switch reason_int {
		case 0:
			reason = "no_state"
		case 1:
			reason = "bad_state"
		case 2:
			reason = "no_gas"
		case 3:
			reason = "suspended"
		}
		if reason != "" {
			transaction.SkippedReason = &reason
		}
	} else {
		transaction.ComputeSuccess = readOptional[bool](compute_phase_data_slice[0])
		transaction.ComputeMsgStateUsed = readOptional[bool](compute_phase_data_slice[1])
		transaction.ComputeAccountActivated = readOptional[bool](compute_phase_data_slice[2])
		transaction.ComputeGasFees = readOptionalNumeric[int64](compute_phase_data_slice[3])
		transaction.ComputeGasUsed = readOptionalNumeric[int64](compute_phase_data_slice[4])
		transaction.ComputeGasLimit = readOptionalNumeric[int64](compute_phase_data_slice[5])
		transaction.ComputeGasCredit = readOptionalNumeric[int64](compute_phase_data_slice[6])
		transaction.ComputeMode = readOptionalNumeric[int16](compute_phase_data_slice[7])
		transaction.ComputeExitCode = readOptionalNumeric[int32](compute_phase_data_slice[8])
		transaction.ComputeExitArg = readOptionalNumeric[int32](compute_phase_data_slice[9])
		transaction.ComputeVmSteps = readOptionalNumeric[int64](compute_phase_data_slice[10])
		transaction.ComputeVmInitStateHash = readOptionalString[string](compute_phase_data_slice[11])
		transaction.ComputeVmFinalStateHash = readOptionalString[string](compute_phase_data_slice[12])
	}
}

func fillBouncePhase(transaction *emulated.TransactionRow, slice []interface{}) {
	bounce_slice := slice[1].([]interface{})
	bounce_phase_type := slice[0].(int8)
	if bounce_phase_type == 0 {
		bounce_type := "negfunds"
		transaction.Bounce = &bounce_type
	} else if bounce_phase_type == 1 {
		bounce_type := "nofunds"
		transaction.Bounce = &bounce_type
		transaction.BounceMsgSizeCells, transaction.BounceMsgSizeBits = readMsgSize(bounce_slice[0].([]interface{}))
		transaction.BounceReqFwdFees = readOptionalNumeric[int64](bounce_slice[1])
	} else if bounce_phase_type == 2 {
		bounce_type := "ok"
		transaction.Bounce = &bounce_type
		transaction.BounceMsgSizeCells, transaction.BounceMsgSizeBits = readMsgSize(bounce_slice[0].([]interface{}))
		transaction.BounceMsgFees = readOptionalNumeric[int64](bounce_slice[1])
		transaction.BounceFwdFees = readOptionalNumeric[int64](bounce_slice[2])
	}
}

func readMsgSize(msg_size_slice []interface{}) (cells *int64, bits *int64) {
	return readOptionalNumeric[int64](msg_size_slice[0]), readOptionalNumeric[int64](msg_size_slice[1])
}

func AssembleFromMessageSlice(slice []interface{}) (msg *emulated.MessageRow, content *emulated.MessageContentRow, init_state *emulated.MessageContentRow, err error) {
	msg = &emulated.MessageRow{}
	content = &emulated.MessageContentRow{}
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered in DecodeMessage", r)
			err = errors.New(fmt.Sprintf("Recovered with: %v", r))
		}
	}()
	msg.MsgHash = slice[0].(string)
	msg.Source = readOptional[string](slice[1])
	msg.Destination = readOptional[string](slice[2])
	msg.Value = readOptionalNumeric[int64](slice[3])
	msg.FwdFee = readOptionalNumeric[int64](slice[4])
	msg.IhrFee = readOptionalNumeric[int64](slice[5])
	msg.CreatedLt = readOptionalNumeric[int64](slice[6])
	msg.CreatedAt = readOptionalNumeric[int64](slice[7])
	msg.Opcode = readOptionalNumeric[int32](slice[8])
	msg.IhrDisabled = readOptional[bool](slice[9])
	msg.Bounce = readOptional[bool](slice[10])
	msg.Bounced = readOptional[bool](slice[11])
	msg.ImportFee = readOptionalNumeric[int64](slice[12])
	if slice[13] != nil {
		content, err = AssembleFromMessageContentString(slice[13].(string))
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if slice[14] != nil {
		init_state, err = AssembleFromMessageContentString(slice[13].(string))
		if err != nil {
			return nil, nil, nil, err
		}
		msg.InitStateHash = &init_state.Hash
	}
	return msg, content, init_state, nil
}

func AssembleFromMessageContentString(data string) (*emulated.MessageContentRow, error) {
	var message_content emulated.MessageContentRow
	body := data
	decodedBody, err := base64.StdEncoding.DecodeString(body)
	message_content.Body = &body
	if err != nil {
		return nil, err
	}
	msg_cell, err := cell.FromBOC(decodedBody)
	if err != nil {
		return nil, err
	}
	hash := msg_cell.Hash()
	// bytes to
	hash_b64 := base64.StdEncoding.EncodeToString(hash[:])
	message_content.Hash = hash_b64
	return &message_content, nil
}

func readOptional[K any](target interface{}) *K {
	if target == nil {
		return nil
	}
	val := target.(K)
	return &val
}

func readOptionalString[K ~string](target interface{}) *K {
	if target == nil {
		return nil
	}
	val := K(target.(string))
	return &val
}

func readAccountStatusChange(val int8) (string, error) {
	switch val {
	case 0:
		return "unchanged", nil
	case 1:
		return "frozen", nil
	case 2:
		return "unfrozen", nil
	}
	return "", errors.New("Unknown account status change: " + strconv.Itoa(int(val)))
}

func readOptionalNumeric[K ~uint64 | ~uint32 | ~int64 | ~int32 | ~int8 | ~int16 | int](target interface{}) *K {
	if target == nil {
		return nil
	}
	switch v := target.(type) {
	case uint64:
		val := K(v)
		return &val
	case uint32:
		val := K(v)
		return &val
	case int64:
		val := K(v)
		return &val
	case int32:
		val := K(v)
		return &val
	case uint8:
		val := K(v)
		return &val
	case int8:
		val := K(v)
		return &val
	case int:
		val := K(v)
		return &val
	case int16:
		val := K(v)
		return &val
	}
	return nil
}

func toAccountStatus(val int) (string, error) {
	switch val {
	case 0:
		return "uninit", nil
	case 1:
		return "frozen", nil
	case 2:
		return "active", nil
	case 3:
		return "frozen", nil
	}
	return "", errors.New("Unknown account status: " + strconv.Itoa(val))
}
