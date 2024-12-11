package emulated

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"reflect"
	"strconv"
)

type TransactionRow struct {
	Account                 string
	Hash                    string
	Lt                      int64
	BlockWorkchain          *int32
	BlockShard              *int64
	BlockSeqno              *int32
	McBlockSeqno            *int32
	TraceID                 *string
	PrevTransHash           *string
	PrevTransLt             *int64
	Now                     *int32
	OrigStatus              *string
	EndStatus               *string
	TotalFees               *int64
	AccountStateHashBefore  *string
	AccountStateHashAfter   *string
	Descr                   *string
	Aborted                 *bool
	Destroyed               *bool
	CreditFirst             *bool
	IsTock                  *bool
	Installed               *bool
	StorageFeesCollected    *int64
	StorageFeesDue          *int64
	StorageStatusChange     *string
	CreditDueFeesCollected  *int64
	Credit                  *int64
	ComputeSkipped          *bool
	SkippedReason           *string
	ComputeSuccess          *bool
	ComputeMsgStateUsed     *bool
	ComputeAccountActivated *bool
	ComputeGasFees          *int64
	ComputeGasUsed          *int64
	ComputeGasLimit         *int64
	ComputeGasCredit        *int64
	ComputeMode             *int16
	ComputeExitCode         *int32
	ComputeExitArg          *int32
	ComputeVmSteps          *int64
	ComputeVmInitStateHash  *string
	ComputeVmFinalStateHash *string
	ActionSuccess           *bool
	ActionValid             *bool
	ActionNoFunds           *bool
	ActionStatusChange      *string
	ActionTotalFwdFees      *int64
	ActionTotalActionFees   *int64
	ActionResultCode        *int32
	ActionResultArg         *int32
	ActionTotActions        *int32
	ActionSpecActions       *int32
	ActionSkippedActions    *int32
	ActionMsgsCreated       *int32
	ActionActionListHash    *string
	ActionTotMsgSizeCells   *int64
	ActionTotMsgSizeBits    *int64
	Bounce                  *string
	BounceMsgSizeCells      *int64
	BounceMsgSizeBits       *int64
	BounceReqFwdFees        *int64
	BounceMsgFees           *int64
	BounceFwdFees           *int64
	SplitInfoCurShardPfxLen *int32
	SplitInfoAccSplitDepth  *int32
	SplitInfoThisAddr       *string
	SplitInfoSiblingAddr    *string
	Emulated                bool
}

type MessageRow struct {
	TxHash        string
	TxLt          int64
	MsgHash       string
	Direction     string
	TraceID       *string
	Source        *string
	Destination   *string
	Value         *int64
	FwdFee        *int64
	IhrFee        *int64
	CreatedLt     *int64
	CreatedAt     *int64
	Opcode        *int32
	IhrDisabled   *bool
	Bounce        *bool
	Bounced       *bool
	ImportFee     *int64
	BodyHash      *string
	InitStateHash *string
}

type MessageContentRow struct {
	Hash string
	Body *string
}

type assign func(dest any) error
type assignable interface {
	getAssigns() []assign
}

type genericRow struct {
	assigns []assign
}

func merge(assignables ...assignable) []assign {
	var assigns []assign
	for _, a := range assignables {
		if a != nil {
			assigns = append(assigns, a.getAssigns()...)
		}
	}
	return assigns
}
func NewRow(assignables ...assignable) pgx.Row {
	return genericRow{assigns: merge(assignables...)}
}
func (r *genericRow) getAssigns() []assign {
	return r.assigns
}
func (r genericRow) Scan(dest ...any) error {
	for i, d := range dest {
		err := r.assigns[i](d)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TransactionRow) getAssigns() []assign {
	return []assign{
		assignString(t.Account),
		assignString(t.Hash),
		assignInt(t.Lt),
		assignIntPtr(t.BlockWorkchain),
		assignIntPtr(t.BlockShard),
		assignIntPtr(t.BlockSeqno),
		assignIntPtr(t.McBlockSeqno),
		assignStringPtr(t.TraceID),
		assignStringPtr(t.PrevTransHash),
		assignIntPtr(t.PrevTransLt),
		assignIntPtr(t.Now),
		assignStringPtr(t.OrigStatus),
		assignStringPtr(t.EndStatus),
		assignIntPtr(t.TotalFees),
		assignStringPtr(t.AccountStateHashBefore),
		assignStringPtr(t.AccountStateHashAfter),
		assignStringPtr(t.Descr),
		assignBoolPtr(t.Aborted),
		assignBoolPtr(t.Destroyed),
		assignBoolPtr(t.CreditFirst),
		assignBoolPtr(t.IsTock),
		assignBoolPtr(t.Installed),
		assignIntPtr(t.StorageFeesCollected),
		assignIntPtr(t.StorageFeesDue),
		assignStringPtr(t.StorageStatusChange),
		assignIntPtr(t.CreditDueFeesCollected),
		assignIntPtr(t.Credit),
		assignBoolPtr(t.ComputeSkipped),
		assignStringPtr(t.SkippedReason),
		assignBoolPtr(t.ComputeSuccess),
		assignBoolPtr(t.ComputeMsgStateUsed),
		assignBoolPtr(t.ComputeAccountActivated),
		assignIntPtr(t.ComputeGasFees),
		assignIntPtr(t.ComputeGasUsed),
		assignIntPtr(t.ComputeGasLimit),
		assignIntPtr(t.ComputeGasCredit),
		assignIntPtr(t.ComputeMode),
		assignIntPtr(t.ComputeExitCode),
		assignIntPtr(t.ComputeExitArg),
		assignIntPtr(t.ComputeVmSteps),
		assignStringPtr(t.ComputeVmInitStateHash),
		assignStringPtr(t.ComputeVmFinalStateHash),
		assignBoolPtr(t.ActionSuccess),
		assignBoolPtr(t.ActionValid),
		assignBoolPtr(t.ActionNoFunds),
		assignStringPtr(t.ActionStatusChange),
		assignIntPtr(t.ActionTotalFwdFees),
		assignIntPtr(t.ActionTotalActionFees),
		assignIntPtr(t.ActionResultCode),
		assignIntPtr(t.ActionResultArg),
		assignIntPtr(t.ActionTotActions),
		assignIntPtr(t.ActionSpecActions),
		assignIntPtr(t.ActionSkippedActions),
		assignIntPtr(t.ActionMsgsCreated),
		assignStringPtr(t.ActionActionListHash),
		assignIntPtr(t.ActionTotMsgSizeCells),
		assignIntPtr(t.ActionTotMsgSizeBits),
		assignStringPtr(t.Bounce),
		assignIntPtr(t.BounceMsgSizeCells),
		assignIntPtr(t.BounceMsgSizeBits),
		assignIntPtr(t.BounceReqFwdFees),
		assignIntPtr(t.BounceMsgFees),
		assignIntPtr(t.BounceFwdFees),
		assignIntPtr(t.SplitInfoCurShardPfxLen),
		assignIntPtr(t.SplitInfoAccSplitDepth),
		assignStringPtr(t.SplitInfoThisAddr),
		assignStringPtr(t.SplitInfoSiblingAddr),
		assignBool(t.Emulated),
	}
}
func (t *TransactionRow) Scan(dest ...any) error {
	assigns := t.getAssigns()
	for i, d := range dest {
		err := assigns[i](d)
		if err != nil {
			return err
		}
	}
	return nil
}
func (m *MessageRow) getAssigns() []assign {
	return []assign{
		assignString(m.TxHash),
		assignInt(m.TxLt),
		assignString(m.MsgHash),
		assignString(m.Direction),
		assignStringPtr(m.TraceID),
		assignStringPtr(m.Source),
		assignStringPtr(m.Destination),
		assignIntPtr(m.Value),
		assignIntPtr(m.FwdFee),
		assignIntPtr(m.IhrFee),
		assignIntPtr(m.CreatedLt),
		assignIntPtr(m.CreatedAt),
		assignIntPtr(m.Opcode),
		assignBoolPtr(m.IhrDisabled),
		assignBoolPtr(m.Bounce),
		assignBoolPtr(m.Bounced),
		assignIntPtr(m.ImportFee),
		assignStringPtr(m.BodyHash),
		assignStringPtr(m.InitStateHash),
	}
}

func (c *MessageContentRow) getAssigns() []assign {
	return []assign{
		assignString(c.Hash),
		assignStringPtr(c.Body),
	}
}
func (m *MessageRow) Scan(dest ...any) error {
	assigns := m.getAssigns()
	for i, d := range dest {
		err := assigns[i](d)
		if err != nil {
			return err
		}
	}
	return nil
}
func assignIntPtr[T int64 | int32 | int16](src *T) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignInt(*src)
}
func assignInt[T int64 | int32 | int16](src T) assign {
	return func(dest any) error {

		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetInt(int64(src))
			break
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dv.SetUint(uint64(src))
			break

		case reflect.String:
			dv.SetString(strconv.FormatInt(int64(src), 10))
			break

		default:
			return fmt.Errorf("unsupported type %T", dest)
		}
		return nil
	}
}
func assignBoolPtr(src *bool) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignBool(*src)
}
func assignBool(src bool) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.Pointer:
			err := assignBool(src)(dv.Interface())
			return err
		case reflect.Bool:
			dv.SetBool(src)
			break
		default:
			return fmt.Errorf("unsupported type %T", dest)
		}
		return nil
	}
}
func assignStringPtr(src *string) assign {
	if src == nil {
		return func(dest any) error {
			return nil
		}
	}
	return assignString(*src)
}
func assignString(src string) assign {
	return func(dest any) error {
		dv := reflect.Indirect(reflect.ValueOf(dest))
		for dv.Kind() == reflect.Ptr {
			dv.Set(reflect.New(dv.Type().Elem()))
			dv = reflect.ValueOf(dv.Interface())
			if dv.Kind() == reflect.Ptr {
				dv = reflect.Indirect(dv)
			}
		}
		switch dv.Kind() {
		case reflect.String:
			dv.SetString(src)
			break
		default:
			return fmt.Errorf("unsupported type %T", dest)
		}
		return nil
	}
}
