import time
from web3 import Web3
from web3.exceptions import ValidationError
from eth_account import Account
from eth_utils import from_wei
from typing import Any, Dict, List, Optional, Tuple, Callable, Union

class SmartContract:
    def __init__(self, abi: List[Dict[str, Any]], contract_address: str, provider: str):
        self.abi = abi
        self.contract_address = Web3.toChecksumAddress(contract_address)
        self.web3 = Web3(Web3.HTTPProvider(provider))
        self.contract = self.web3.eth.contract(
            address=self.contract_address, abi=self.abi)

        self.abi_map = {
            abi_entry["name"]: abi_entry
            for abi_entry in self.abi if abi_entry["type"] == "function"
        }

    def cast_output_types(self, raw_result, output_abi):
        if "components" not in output_abi:
            return raw_result

        if output_abi["type"].startswith("tuple"):
            return {
                (sub_abi["name"] or str(i)): self.cast_output_types(item, sub_abi)
                for i, (item, sub_abi) in enumerate(zip(raw_result, output_abi["components"]))
            }
        elif output_abi["type"].startswith("array"):
            return [
                self.cast_output_types(item, output_abi["components"][0])
                for item in raw_result
            ]
        else:
            return raw_result

    def call_function(self, function_name: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        contract_function = self.contract.get_function_by_name(function_name)
        output_abi = self.abi_map[function_name]["outputs"]

        raw_result = contract_function(*args, **kwargs).call()

        if not isinstance(raw_result, tuple):
            raw_result = (raw_result,)

        result = self.cast_output_types(raw_result, output_abi)

        return result

    def cast_single_value(self, value: Any, output_type: str) -> Any:
        if output_type.startswith("uint"):
            return int(value)
        elif output_type == "address":
            return str(value)
        elif output_type.startswith("bytes"):
            return bytes(value).rstrip(b'\x00').decode()
        elif output_type.endswith("[]"):
            return [self.cast_single_value(item, output_type[:-2]) for item in value]
        else:
            return value

    def get_output_types_and_names(self, function_name: str) -> Tuple[List[str], List[str]]:
        function_abi = next(
            abi for abi in self.abi if abi['type'] == 'function' and abi['name'] == function_name)
        output_types = [output['type'] for output in function_abi['outputs']]
        output_names = [output['name'] for output in function_abi['outputs']]

        return output_types, output_names

    def send_transaction(self, function_name: str, private_key: Optional[str] = None, gas: Optional[int] = None, *args: Any, **kwargs: Any) -> str:
        if not private_key:
            raise ValueError(
                "You must provide a private key")

        if private_key:
            account = Account.privateKeyToAccount(private_key)

        contract_function = self.contract.get_function_by_name(function_name)
        transaction = contract_function(*args, **kwargs).buildTransaction({
            'chainId': self.web3.eth.chain_id,
            'gas': gas or self.estimate_gas(function_name, *args, **kwargs),
            'gasPrice': self.web3.eth.gasPrice,
            'nonce': self.web3.eth.getTransactionCount(account.address),
        })
        return transaction

    def send_transaction_and_wait(self, function_name: str, on_failure: Callable, private_key: Optional[str] = None, gas: Optional[int] = None, *args: Any, **kwargs: Any) -> None:
        txn_hash = self.send_transaction(
            function_name, private_key=private_key, gas=gas, *args, **kwargs)
        print(f"Transaction hash: {txn_hash}")

        while True:
            try:
                txn_receipt = self.web3.eth.getTransactionReceipt(txn_hash)
            except ValidationError:
                txn_receipt = None

            if txn_receipt is None:
                print("Waiting for transaction to be mined...")
                time.sleep(10)
            else:
                break

        if txn_receipt['status'] == 1:
            print("Transaction succeeded")
            print(f"Block number: {txn_receipt['blockNumber']}")
            print(f"Gas used: {txn_receipt['gasUsed']}")
        else:
            print("Transaction failed")
            on_failure()

    def estimate_gas(self, function_name: str, *args: Any, **kwargs: Any) -> int:
        contract_function = self.contract.get_function_by_name(function_name)
        try:
            gas_estimate = contract_function(*args, **kwargs).estimateGas()
            return gas_estimate
        except ValidationError as e:
            raise ValueError(f"Failed to estimate gas: {e}")

    def _is_wei_value(self, name: str) -> bool:
        wei_keywords = ['wei', 'eth']
        return any(keyword in name.lower() for keyword in wei_keywords)
