/* DPI Token balances of new holders among POAP addresses */
/* https://dune.xyz/queries/209954/401812 */

with 
poap_addresses AS (

    SELECT * FROM (VALUES
        ('\x0100e4f2a841e773564c333f889ab6d6bd5fcb22'),
        ('\x05ba910d9b6128da5e2b9486d5edc00f8174f1c6'),
        ('\x05c9508aa6156f10bf9a7e43c080cc6eb3971e99'),
        ('\x06334ce2436d281359a74f8163186f4fa2e942f4'),
        ('\x08c6cab5bf0ae77833c5cbf8d2dd934a9253cd7b'),
        ('\x128dd6789a5ca88d46ea9861ac95d4a3d876539d'),
        ('\x1ef8019a1793ef3283742d7aa975b9a66133a2d0'),
        ('\x21bc7fbf89b07e318dce55676bff0c3ff5edb948'),
        ('\x29caeea733088cf59db91c673c66b50f390e8183'),
        ('\x2cdcf4ea746fe053737978e7e10807698f21e814'),
        ('\x2dbf79ad0ba63898b9fa414c35be03fb074972ba'),
        ('\x33bd61af24582c8742264d6a06f876fba211ef60'),
        ('\x35675299d9a0891da3597f9d1317ac2ca5c9c2ac'),
        ('\x358cef6068733f92b8c87e2dad0a03e0cc5f281c'),
        ('\x3f5b5d7d68164628c4b61ca3eccc4f877396f993'),
        ('\x41959aacd08402eca8f5c290a5643fc84d60eb7d'),
        ('\x421f223e19877d9765ecd6e8ec4812457229b36a'),
        ('\x428c210c2eb982a199d0f5d4cfcc4852c0519274'),
        ('\x480b2701a29a737b2f15f4876c392bd96a75d453'),
        ('\x49ccac5ed5715e34dacb3ae81022fa561efd9191'),
        ('\x4a35677c1b8450cd27b619ac7356a9eeeb0b4368'),
        ('\x4c3ada723a2d63eaf5d7225d41de1b96fe700a14'),
        ('\x4f8c2d5397262653cd8956cb977a0ba3660210c7'),
        ('\x53afc742f1de3be40f3693b3e37e6b0926dd2f91'),
        ('\x546c1528319f9fc621f5752d98690f28293a5c1d'),
        ('\x578152463e01de0fc1331250351dd6d11dafd9b3'),
        ('\x57f84c67bc0d85043858d9ebcc8f3d35bd336f5b'),
        ('\x5955f5b33e67571110ee1e40e540c072be63d094'),
        ('\x5eee4c61d5e63486dcd3eb4ad445403c9e1bb413'),
        ('\x635485da38e44eed324077760f17620e3d3991d4'),
        ('\x6626160e5e476a936d3d46f8bcb60414c0183410'),
        ('\x6b1050c1c6b288c79ac1db299dc481048abbbbcd'),
        ('\x7177494158c6a27b18f7aa485dd58f852c9fcaad'),
        ('\x7721f140c2968d5c639a9b40a1e6ca48a9b7c41d'),
        ('\x775936c4dca762d38e329930c60dc4a71b724ca1'),
        ('\x7b13e920e92688947819970d064c0a44afbf9b07'),
        ('\x7ee40e56c015832beb3c5b92dc8483e322ee5932'),
        ('\x7eec17130e51a067993f9b47d757987657b1ba6b'),
        ('\xd7e7bca98ab9fb25e17ad73429e89a40b55708be'),
        ('\x7fa0bbf4856bf37ea8d0e9d1b47514abf17beb84'),
        ('\xa6e59b844891e619801b298f4f0af52054513a3c'),
        ('\xe7fe2a9d8fa0f37b33f994dff5b4a3219fe343a2'),
        ('\x84511ca923bdb5f4b6ecf7a5b147f58767bf6c8e'),
        ('\x875a89c827b2c62688d6d4009c7c537799fd7fa2'),
        ('\x9b2829c0c4203e2ee0d9d61f94aa724271705a02'),
        ('\x9d7d2d5c305348faf3aa185d7114dcdd936d5b45'),
        ('\x9e49b413bf488202d21fbae112256509f41effd6'),
        ('\xa0e27626cb0f54a717ee3315b2a592e2bfbe7f48'),
        ('\xa6685809ad01cf447f81780e29529a331cdeadfe'),
        ('\xa86f7cb847bf41b049002cb0c96e6156a8b27e25'),
        ('\xae447efdb02d15d460d9c640710244f3ebad5473'),
        ('\xb462521ead6822caacec001daac0978d057dd611'),
        ('\xb7b01d03a6cdcb85a41368e0cd94f4dac1418536'),
        ('\xb8cf2227a96bd9f32e7a138733cf891c2f89ed17'),
        ('\xbea6c46af03c4ef9c9d96c810c17169651b1ed60'),
        ('\xc42705a210f082ff29e6beac80f56c41f0a54091'),
        ('\xd5513ddb9780e610c56ae32a29dafc3c7abc0a8c'),
        ('\xd5835db0959569622681dcff1e72b0936170bd6b'),
        ('\xd76ff76ac8019c99237bde08d7c39dab5481bed2'),
        ('\xd8895c04cb7d43ed16d83268d91cf946fffa4254'),
        ('\xd8d7c1148fc72638532e8b4ff3f1934fb7a08ed6'),
        ('\xda3372784273811144284e92fafe7f5dc6e4aa3d'),
        ('\xdad3fd6c9fb0c2b56228e58ae191b62bfb1bec83'),
        ('\xdb26aa474fc303b6757b327dd51a73cb8ae97987'),
        ('\xdb52f6a6c1f6d4f53b91f9ec6653e0dcc7bdbc00'),
        ('\xdd787ddbc72ea62e7c644b6206a23c05a5f9d487'),
        ('\xde5f94136b342f0f662f4be09b67dbb3ebf8f2dc'),
        ('\xf29dbc79ae222677f8a91dbe55cf052e61c206fd'),
        ('\xf6b7af3eaed85198ad9c49353485d17b54988e7a'),
        ('\xfe760978a84b58259a4c851b42bd8c9c7ef93e80'),
        ('\xff931c9f1ccaa5e6db3e52c52b64d39c842c5daa'),
        ('\xe9af62eb3dc36f4a39b52d2b3e06f09295bb1680'),
        ('\x9f2880427f86a15daedc4c6f3185e8affe2ba761'),
        ('\x57efcc7607cd2da49d73e8f9c88ed114cbdc5cf7'),
        ('\xb63a90e0dcfc1ee94c7b2dd827b1c7f68dfbac89'),
        ('\x2b384212edc04ae8bb41738d05ba20e33277bf33'),
        ('\x6a19891e91a3d4d690cc8f9627290d25bfd34df6'),
        ('\x1c956943024dd561ad820a75ac374922d21dbcda'),
        ('\x47b64ae719e7ce8bfef1627c4c58c5477792fb60'),
        ('\xe64b1a000b931080a73546d4ed5d742495ce2a92'),
        ('\x20b6fa95a915ec3ce053fa1d3a5759cb9118137a'),
        ('\x95ace38b839597bcbdc5d357776b18cb5d501cf5'),
        ('\x53fd9fe0837a281d02f91b61fd7ce2f7b60566bd'),
        ('\xb01474b50382fae1a847e3a916ecdf07ba57bcc7'),
        ('\xd338f79ce0615f7decba58fa3b71f215758b5406'),
        ('\x5f2091da87586684d69ce4d6f8e1897a0ac588eb'),
        ('\x2e8abfe042886e4938201101a63730d04f160a82'),
        ('\x32511f960f4b380cdde599065019094b309c2ce8'),
        ('\x7a74495d0f52683d5c0b04f804ed0b5efa083bd0'),
        ('\xe7708176a1464f34372bb35366e347811b6b5a26')
            ) AS t (address)
            
), 
poap_holder_balances AS (Select address,  amount, day, token_symbol
from poap_addresses pa
left join erc20."view_token_balances_daily" tb on tb.wallet_address = pa.address::bytea
where token_address = '\x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b'
),

max_balance_before_event AS (
 SELECT MAX(amount) as max_balance, address FROM poap_holder_balances WHERE day < '2021-07-15' GROUP BY address 
),

wallets_without_balance_before_event AS (SELECT pa.address FROM poap_addresses pa LEFT JOIN max_balance_before_event mb ON pa.address = mb.address WHERE max_balance ISNULL OR max_balance = 0)

SELECT SUM(amount) as total_dpi_balance, COUNT(DISTINCT w.address) as holders, day
FROM wallets_without_balance_before_event w 
JOIN poap_holder_balances b 
ON w.address = b.address 
WHERE day >= '2021-07-15' AND amount > 0
GROUP BY day
ORDER BY day DESC;



