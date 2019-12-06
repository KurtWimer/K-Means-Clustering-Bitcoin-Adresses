#!/bin/bash

wget https://www.walletexplorer.com/wallet/Bittrex.com?format=csv -O ->> exchanges.txt

wget https://www.walletexplorer.com/wallet/BTCCPool?format=csv -O ->> pool.txt

wget https://www.walletexplorer.com/wallet/Xapo.com?format=csv -O ->> services.txt

wget https://www.walletexplorer.com/wallet/LuckyB.it?format=csv -O ->> gambling.txt
