from pydantic import BaseModel
from typing import Optional

class PedidoFiltro(BaseModel):
    tipo_produto: Optional[str] = None
    mes_do_ano_num: Optional[int] = None
    canal: Optional[str] = None
    regiao_destino: Optional[str] = None
    cliente_segmento: Optional[str] = None
