package main

import (
	"blast/blockchain"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

type L1BlockRef struct {
	Hash       common.Hash `json:"hash"`
	Number     uint64      `json:"number"`
	ParentHash common.Hash `json:"parentHash"`
	Time       uint64      `json:"timestamp"`
}

// IndexedBlobHash represents a blob hash that commits to a single blob confirmed in a block.  The
// index helps us avoid unnecessary blob to blob hash conversions to find the right content in a
// sidecar.
type IndexedBlobHash struct {
	Index uint64      // absolute index in the block, a.k.a. position in sidecar blobs array
	Hash  common.Hash // hash of the blob, used for consistency checks
}

type BlobsStore struct {
	// block timestamp -> blob versioned hash -> blob
	blobs map[uint64]map[IndexedBlobHash]*kzg4844.Blob
}

func NewBlobStore() *BlobsStore {
	return &BlobsStore{blobs: make(map[uint64]map[IndexedBlobHash]*kzg4844.Blob)}
}

func (store *BlobsStore) StoreBlob(blockTime uint64, indexedHash IndexedBlobHash, blob *kzg4844.Blob) {
	m, ok := store.blobs[blockTime]
	if !ok {
		m = make(map[IndexedBlobHash]*kzg4844.Blob)
		store.blobs[blockTime] = m
	}
	m[indexedHash] = blob
}

func (store *BlobsStore) GetBlobs(
	ctx context.Context, ref L1BlockRef, hashes []IndexedBlobHash,
) ([]*kzg4844.Blob, error) {
	out := make([]*kzg4844.Blob, 0, len(hashes))
	m, ok := store.blobs[ref.Time]
	if !ok {
		return nil, fmt.Errorf("no blobs known with given time: %w", ethereum.NotFound)
	}
	for _, h := range hashes {
		b, ok := m[h]
		if !ok {
			return nil, fmt.Errorf("blob %d %s is not in store: %w", h.Index, h.Hash, ethereum.NotFound)
		}
		out = append(out, b)
	}
	return out, nil
}

// func (store *BlobsStore) GetBlobSidecars(ctx context.Context, ref L1BlockRef, hashes []IndexedBlobHash) ([]*eth.BlobSidecar, error) {
// 	out := make([]*eth.BlobSidecar, 0, len(hashes))
// 	m, ok := store.blobs[ref.Time]
// 	if !ok {
// 		return nil, fmt.Errorf("no blobs known with given time: %w", ethereum.NotFound)
// 	}
// 	for _, h := range hashes {
// 		b, ok := m[h]
// 		if !ok {
// 			return nil, fmt.Errorf("blob %d %s is not in store: %w", h.Index, h.Hash, ethereum.NotFound)
// 		}
// 		if b == nil {
// 			return nil, fmt.Errorf("blob %d %s is nil, cannot copy: %w", h.Index, h.Hash, ethereum.NotFound)
// 		}

// 		commitment, err := kzg4844.BlobToCommitment(b.KZGBlob())
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to convert blob to commitment: %w", err)
// 		}
// 		proof, err := kzg4844.ComputeBlobProof(b.KZGBlob(), commitment)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to compute blob proof: %w", err)
// 		}
// 		out = append(out, &eth.BlobSidecar{
// 			Index:         eth.Uint64String(h.Index),
// 			Blob:          *b,
// 			KZGCommitment: eth.Bytes48(commitment),
// 			KZGProof:      eth.Bytes48(proof),
// 		})
// 	}
// 	return out, nil
// }

// func (store *BlobsStore) GetAllSidecars(ctx context.Context, l1Timestamp uint64) ([]*eth.BlobSidecar, error) {
// 	m, ok := store.blobs[l1Timestamp]
// 	if !ok {
// 		return nil, fmt.Errorf("no blobs known with given time: %w", ethereum.NotFound)
// 	}
// 	out := make([]*eth.BlobSidecar, len(m))
// 	for h, b := range m {
// 		if b == nil {
// 			return nil, fmt.Errorf("blob %d %s is nil, cannot copy: %w", h.Index, h.Hash, ethereum.NotFound)
// 		}

// 		commitment, err := kzg4844.BlobToCommitment(b.KZGBlob())
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to convert blob to commitment: %w", err)
// 		}
// 		proof, err := kzg4844.ComputeBlobProof(b.KZGBlob(), commitment)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to compute blob proof: %w", err)
// 		}
// 		out[h.Index] = &eth.BlobSidecar{
// 			Index:         eth.Uint64String(h.Index),
// 			Blob:          *b,
// 			KZGCommitment: eth.Bytes48(commitment),
// 			KZGProof:      eth.Bytes48(proof),
// 		}
// 	}
// 	return out, nil
// }

type pluginBlast struct {
	log hclog.Logger

	node         *node.Node
	Eth          *eth.Ethereum
	prefCoinbase common.Address

	blobStore *BlobsStore

	// L1 evm / chain
	l1Chain    *core.BlockChain
	l1Database ethdb.Database
	l1Cfg      *core.Genesis
	l1Signer   types.Signer
	// current work state - only non-nil when making a block
	s *workState
}

type workState struct {
	l1BuildingHeader       *types.Header
	l1BuildingState        *state.StateDB
	l1Receipts             []*types.Receipt
	L1Transactions         []*types.Transaction
	pendingIndices         map[common.Address]uint64
	l1TxFailed             []*types.Transaction // log of failed transactions which could not be included
	l1BuildingBlobSidecars []*types.BlobTxSidecar
	L1GasPool              *core.GasPool
}

// force compliation fail
var (
	_ blockchain.Chain = (*pluginBlast)(nil)
)

// NOTE NOTE NOTE all errors returned MUST be wrapped with plugin.NewBasicError!

func (p *pluginBlast) InitExtraConfigs(cfg []byte) error {

	return nil
}

func (p *pluginBlast) IncludeTxByHash(hexHash string) error {
	hsh := common.HexToHash(hexHash)
	if hsh == (common.Hash{}) {
		return plugin.NewBasicError(ErrDidNotHaveTx)
	}
	tx := p.Eth.TxPool().Get(hsh)
	if tx == nil {
		return plugin.NewBasicError(fmt.Errorf("cannot find tx %s", hsh))
	}
	p.log.Info("had tx requested - now will process tx")
	if err := p.includeTx(tx); err != nil {
		return plugin.NewBasicError(err)
	}
	from, err := p.l1Signer.Sender(tx)
	if err != nil {
		return plugin.NewBasicError(err)
	}
	p.s.pendingIndices[from] = p.s.pendingIndices[from] + 1 // won't retry the tx
	return nil
}

func (p *pluginBlast) includeTx(tx *types.Transaction) error {
	from, err := p.l1Signer.Sender(tx)
	if err != nil {
		return plugin.NewBasicError(err)
	}
	p.log.Info("including tx", "nonce", tx.Nonce(), "from", from, "to", tx.To())
	if tx.Gas() > p.s.l1BuildingHeader.GasLimit {
		return plugin.NewBasicError(
			fmt.Errorf("tx consumes %d gas, more than available in L1 block %d", tx.Gas(), p.s.l1BuildingHeader.GasLimit),
		)
	}

	if tx.Gas() > uint64(*p.s.L1GasPool) {
		return plugin.NewBasicError(fmt.Errorf("action takes too much gas: %d, only have %d", tx.Gas(), uint64(*p.s.L1GasPool)))
	}

	p.s.l1BuildingState.SetTxContext(tx.Hash(), len(p.s.L1Transactions))
	p.log.Info("about to apply tx", "hsh", tx.Hash())
	st := time.Now()

	blkCtx := core.NewEVMBlockContext(p.s.l1BuildingHeader, p.l1Chain, &p.s.l1BuildingHeader.Coinbase)
	newEVM := vm.NewEVM(blkCtx, p.s.l1BuildingState, p.l1Cfg.Config, *p.l1Chain.GetVMConfig())
	receipt, err := core.ApplyTransaction(
		newEVM, p.s.L1GasPool, p.s.l1BuildingState, p.s.l1BuildingHeader, tx, &p.s.l1BuildingHeader.GasUsed,
	)

	p.log.Info("applied tx", "hsh", tx.Hash().Hex(), "took", time.Since(st))

	if err != nil {
		p.s.l1TxFailed = append(p.s.l1TxFailed, tx)
		return plugin.NewBasicError(fmt.Errorf("failed to apply transaction to L1 block (tx %d): %v", len(p.s.L1Transactions), err))
	}

	p.s.l1Receipts = append(p.s.l1Receipts, receipt)
	p.s.L1Transactions = append(p.s.L1Transactions, tx.WithoutBlobTxSidecar())

	if tx.Type() == types.BlobTxType {
		if !p.l1Cfg.Config.IsCancun(p.s.l1BuildingHeader.Number, p.s.l1BuildingHeader.Time) {
			return plugin.NewBasicError(ErrNotCancunCantDoBlob)
		}
		sidecar := tx.BlobTxSidecar()
		if sidecar != nil {
			p.s.l1BuildingBlobSidecars = append(p.s.l1BuildingBlobSidecars, sidecar)
		}
		*p.s.l1BuildingHeader.BlobGasUsed += receipt.BlobGasUsed
	}

	return nil
}

func (p *pluginBlast) Close() error {
	p.log.Info("blast plugin about to call close on node")
	// silly workaround because gob encoding, just leave as is whatever
	if err := plugin.NewBasicError(p.node.Close()); err != nil {
		return err
	}
	return nil
}

func (p *pluginBlast) EndBlock() blockchain.NewBlockOrError {
	p.log.Debug("ending currently pending block")

	p.s.l1BuildingHeader.GasUsed = p.s.l1BuildingHeader.GasLimit - uint64(*p.s.L1GasPool)
	p.s.l1BuildingHeader.Root = p.s.l1BuildingState.IntermediateRoot(p.l1Cfg.Config.IsEIP158(p.s.l1BuildingHeader.Number))

	var withdrawals []*types.Withdrawal
	if p.l1Cfg.Config.IsShanghai(p.s.l1BuildingHeader.Number, p.s.l1BuildingHeader.Time) {
		withdrawals = make([]*types.Withdrawal, 0)
	}

	if p.l1Chain.Config().IsPrague(p.s.l1BuildingHeader.Number, p.s.l1BuildingHeader.Time) {
		p.s.l1BuildingHeader.RequestsHash = &types.EmptyRequestsHash
	}

	block := types.NewBlock(
		p.s.l1BuildingHeader, &types.Body{
			Transactions: p.s.L1Transactions,
			Withdrawals:  withdrawals}, p.s.l1Receipts, trie.NewStackTrie(nil),
	)
	isCancun := p.l1Cfg.Config.IsCancun(p.s.l1BuildingHeader.Number, p.s.l1BuildingHeader.Time)

	// Write state changes to db
	root, err := p.s.l1BuildingState.Commit(
		p.s.l1BuildingHeader.Number.Uint64(), p.l1Cfg.Config.IsEIP158(p.s.l1BuildingHeader.Number), isCancun,
	)
	if err != nil {
		p.log.Error("problem-1", "err", err)
		return blockchain.NewBlockOrError{Err: *plugin.NewBasicError(err)}
	}

	if err := p.s.l1BuildingState.Database().TrieDB().Commit(root, false); err != nil {
		p.log.Error("problem-2", "err", err)
		return blockchain.NewBlockOrError{Err: *plugin.NewBasicError(err)}
	}

	// now that the blob txs are in a canonical block, flush them to the blob store
	for _, sidecar := range p.s.l1BuildingBlobSidecars {
		for i, h := range sidecar.BlobHashes() {
			blob := sidecar.Blobs[i]
			indexedHash := IndexedBlobHash{Index: uint64(i), Hash: h}
			p.blobStore.StoreBlob(block.Time(), indexedHash, &blob)
		}
	}

	_, err = p.l1Chain.InsertChain(types.Blocks{block})
	if err != nil {
		p.log.Error("problem-3", "err", err)
		return blockchain.NewBlockOrError{Err: *plugin.NewBasicError(err)}
	}

	p.s = nil

	serialized, err := json.Marshal(struct {
		Hdr      *types.Header
		Txs      types.Transactions
		Receipts types.Receipts
	}{Hdr: block.Header(), Txs: block.Transactions()})

	if err != nil {
		return blockchain.NewBlockOrError{Err: *plugin.NewBasicError(err)}
	}

	return blockchain.NewBlockOrError{SerializedBlock: serialized}

}

// TODO error if its not started yet?
func (p *pluginBlast) WSEndpoint() (string, error) {
	return p.node.WSEndpoint(), nil
}

var (
	ErrEmptyAddr           = errors.New("empty addr")
	ErrDidNotHaveTx        = errors.New("did not have tx by hash")
	ErrNotCancunCantDoBlob = errors.New("not cancun yet so cant do blob")
)

func (p *pluginBlast) NewChain(startingArgs *blockchain.NewChainStartingArgs) ([]byte, error) {
	var gen *core.Genesis

	p.log.Info(
		"making new chain in geth as plugin",
		"using-custom-genesis", len(startingArgs.SerializedGenesis) > 0,
	)

	if startingArgs.AssumeMainnet {
		// Then assume eth mainnet
		gen = core.DefaultGenesisBlock()
	} else if len(startingArgs.SerializedGenesis) > 0 {
		if err := json.Unmarshal(startingArgs.SerializedGenesis, &gen); err != nil {
			return nil, plugin.NewBasicError(fmt.Errorf("problem deserializing genesis %w", err))
		}

		// l1Blk := gen.ToBlock()
		// how it got it badck
		// pr, _ := json.MarshalIndent([]any{l1Blk.Header(), l1Blk, gen}, " ", " ")
		//		fmt.Fprintln(os.Stderr, "in plugin", string(pr))
		// os.WriteFile("TEMP_GEN_TEST.json", pr, 0644)
		blockZeroHash := gen.ToBlock().Hash()

		p.log.Info(
			"custom genesis provided blob schedule",
			"assuming-block-zero-hash", blockZeroHash,
			"schedule", gen.Config.BlobScheduleConfig,
			//			"genesis", gen,
		)

	} else {
		gen = core.DeveloperGenesisBlock(30_000_000, nil)
		gen.Config.CancunTime = startingArgs.WhenActivateCancun
		gen.Config.PragueTime = startingArgs.WhenActivatePrague
		for _addr, amt := range startingArgs.ExtraAllocs {
			addr := common.HexToAddress(_addr)
			if addr == (common.Address{}) {
				return nil, plugin.NewBasicError(ErrEmptyAddr)
			}
			gen.Alloc[addr] = types.Account{
				Balance: amt,
			}
		}

	}

	ethCfg := &ethconfig.Config{
		NetworkId:   gen.Config.ChainID.Uint64(),
		Genesis:     gen,
		StateScheme: rawdb.HashScheme,
		NoPruning:   true,
	}
	nodeCfg := &node.Config{
		Name:        "real-l1-geth",
		WSHost:      "127.0.0.1",
		WSPort:      0,
		HTTPHost:    "127.0.0.1",
		HTTPPort:    0,
		WSModules:   []string{"debug", "admin", "eth", "txpool", "net", "rpc", "web3", "personal"},
		HTTPModules: []string{"debug", "admin", "eth", "txpool", "net", "rpc", "web3", "personal"},
		DataDir:     "", // in-memory
		P2P:         p2p.Config{NoDiscovery: true, NoDial: true},
	}

	n, err := node.New(nodeCfg)
	if err != nil {
		return nil, plugin.NewBasicError(err)
	}

	backend, err := eth.New(n, ethCfg)
	if err != nil {
		return nil, plugin.NewBasicError(err)
	}

	n.RegisterAPIs(tracers.APIs(backend.APIBackend))
	filterSystem := filters.NewFilterSystem(backend.APIBackend, filters.Config{
		LogCacheSize: ethCfg.FilterLogCacheSize,
	})
	n.RegisterAPIs([]rpc.API{
		{Namespace: "eth", Service: filters.NewFilterAPI(filterSystem)},
		// IF YOU DO THIS THEN IT BREAKS!
		// {Namespace: "eth", Service: backend.APIBackend},
	})

	p.node = n
	p.Eth = backend
	p.l1Chain = backend.BlockChain()
	p.l1Database = backend.ChainDb()
	p.l1Cfg = gen
	p.l1Signer = types.LatestSigner(gen.Config)
	err = n.Start()
	if err != nil {
		return nil, plugin.NewBasicError(err)
	}

	return json.Marshal(struct {
		Hdr *types.Header
	}{Hdr: gen.ToBlock().Header()})
}

func (p *pluginBlast) SetFeeRecipient(addr string) error {
	coinbase := common.HexToAddress(addr)
	if coinbase == (common.Address{}) {
		return plugin.NewBasicError(fmt.Errorf("fee recipient set to empty addr"))
	}
	p.prefCoinbase = coinbase
	return nil
}

func (p *pluginBlast) StartBlock(timeDelta uint64) error {
	p.log.Debug("plugin started new block")
	parent := p.l1Chain.CurrentHeader()
	parentHash := parent.Hash()
	statedb, err := state.New(parent.Root, state.NewDatabase(triedb.NewDatabase(p.l1Database, nil), nil))
	if err != nil {
		return plugin.NewBasicError(
			fmt.Errorf("failed to init state db around block %s (state %s): %w", parentHash, parent.Root, err),
		)
	}
	header := &types.Header{
		ParentHash: parentHash,
		Coinbase:   p.prefCoinbase,
		Difficulty: common.Big0,
		Number:     new(big.Int).Add(parent.Number, common.Big1),
		GasLimit:   parent.GasLimit,
		Time:       parent.Time + timeDelta,
		Extra:      []byte("L1 was here"),
		MixDigest:  common.Hash{}, // TODO: maybe randomize this (prev-randao value)
	}

	if p.l1Cfg.Config.IsLondon(header.Number) {
		header.BaseFee = eip1559.CalcBaseFee(p.l1Cfg.Config, parent)
		// At the transition, double the gas limit so the gas target is equal to the old gas limit.
		if !p.l1Cfg.Config.IsLondon(parent.Number) {
			header.GasLimit = parent.GasLimit * p.l1Cfg.Config.ElasticityMultiplier()
		}
	}

	if p.l1Cfg.Config.IsShanghai(header.Number, header.Time) {
		header.WithdrawalsHash = &types.EmptyWithdrawalsHash
	}

	if p.l1Cfg.Config.IsCancun(header.Number, header.Time) {

		var excessBlobGas uint64
		if p.l1Cfg.Config.IsCancun(parent.Number, parent.Time) {
			excessBlobGas = eip4844.CalcExcessBlobGas(p.l1Cfg.Config, parent, header.Time)
		}

		header.BlobGasUsed = new(uint64)
		header.ExcessBlobGas = &excessBlobGas
		root := crypto.Keccak256Hash([]byte("fake-beacon-block-root"), header.Number.Bytes())
		header.ParentBeaconRoot = &root

		context := core.NewEVMBlockContext(header, p.l1Chain, nil)
		vmenv := vm.NewEVM(context, statedb, p.l1Chain.Config(), vm.Config{})

		if header.ParentBeaconRoot != nil {
			core.ProcessBeaconBlockRoot(*header.ParentBeaconRoot, vmenv)
		}
	}

	p.s = &workState{
		l1BuildingHeader: header,
		l1BuildingState:  statedb,
		l1Receipts:       make([]*types.Receipt, 0),
		L1Transactions:   make([]*types.Transaction, 0),
		pendingIndices:   make(map[common.Address]uint64),
		//		l1BuildingBlobSidecars: make([]*types.BlobTxSidecar, 0),
		L1GasPool: new(core.GasPool).AddGas(header.GasLimit),
	}

	p.log.Debug("work state ready")

	return nil
}

var handshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "BASIC_PLUGIN",
	MagicCookieValue: "hello",
}

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:           hclog.Info,
		Output:          os.Stderr,
		JSONFormat:      false,
		Color:           hclog.AutoColor,
		IncludeLocation: true,
	})

	chain := &pluginBlast{
		log: logger,
	}

	pluginMap := map[string]plugin.Plugin{
		"blast": &blockchain.ChainPlugin{Impl: chain},
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
