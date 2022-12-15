import bz2
import logging
import pickle

import radix


class IpLookup:
    NOT_FOUND = '0'

    def __init__(self, rib_file: str,
                 pdb_file: str,
                 bgp_timestamp: int = 0,
                 pdb_timestamp: int = 0) -> None:
        self.bgp_ip2asn = radix.Radix()
        self.pdb_ip2asn = dict()
        self.pdb_ip2ix = radix.Radix()
        self.__init_bgp(rib_file)
        self.__init_pdb(pdb_file)
        self.rib_timestamp = bgp_timestamp
        self.pdb_timestamp = pdb_timestamp

    def __init_bgp(self, rib_file: str) -> None:
        with bz2.open(rib_file, 'rb') as f:
            self.bgp_ip2asn = pickle.load(f)

    def __init_pdb(self, pdb_file: str) -> None:
        with bz2.open(pdb_file, 'rb') as f:
            data = pickle.load(f)
        self.pdb_ip2asn = data['participants_dict']
        self.pdb_ip2ix = data['rtree']

    def replace_bgp(self, rib_file: str, bgp_timestamp: int = 0) -> None:
        self.bgp_ip2asn = radix.Radix()
        self.__init_bgp(rib_file)
        self.rib_timestamp = bgp_timestamp

    def replace_pdb(self, pdb_file: str, pdb_timestamp: int = 0) -> None:
        self.pdb_ip2asn = dict()
        self.pdb_ip2ix = radix.Radix()
        self.__init_pdb(pdb_file)
        self.pdb_timestamp = pdb_timestamp

    def ip2asn(self, ip: str) -> str:
        try:
            node = self.bgp_ip2asn.search_best(ip)
        except ValueError as e:
            logging.error(f'Invalid IP: {ip} {e}')
            return self.NOT_FOUND
        if node:
            logging.debug('ip2asn bgp')
            return node.data['as']
        if ip in self.pdb_ip2asn:
            logging.debug('ip2asn pdb')
            return self.pdb_ip2asn[ip]
        return self.NOT_FOUND

    def ip2prefix(self, ip: str) -> str:
        try:
            node = self.bgp_ip2asn.search_best(ip)
            if node:
                logging.debug('ip2prefix bgp')
                return node.prefix
            node = self.pdb_ip2ix.search_best(ip)
            if node:
                logging.debug('ip2prefix pdb')
                return node.prefix
        except ValueError as e:
            logging.error(f'Invalid IP: {ip} {e}')
            return self.NOT_FOUND
        return self.NOT_FOUND

    def ip2ix(self, ip: str) -> str:
        try:
            node = self.pdb_ip2ix.search_best(ip)
        except ValueError as e:
            logging.error(f'Invalid IP: {ip} {e}')
            return self.NOT_FOUND
        if node:
            return node.data['id']
        return self.NOT_FOUND
